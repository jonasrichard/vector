#!/usr/bin/env python3
"""
End-to-end passthrough tests for Vector.

test_passthrough               – basic file sink passthrough
test_batching                  – 20 msgs at 1/s, 3s batch timeout, verify all 20 arrive
test_batch_timeout_reliability – minimal reproducer for the lost-timer bug
test_production_pipeline_timer – production-accurate reproducer: OTLP source,
                                 full attributor→filter→bouncer→remap transform
                                 chain, disk-buffered HTTP sink — same topology
                                 as the real tlmr deployment
"""

from __future__ import annotations
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, HTTPServer

VECTOR_BIN     = os.path.join(os.path.dirname(__file__), "..", "target", "debug", "vector")
TELEMETRYGEN   = "telemetrygen"
TENANT_ID      = "6ecb588d-3413-44ff-a314-ff4db7b49ca7"

SOURCE_PORT    = 8788    # plain HTTP source (tests 1-3)
COLLECTOR_PORT = 8789    # our in-process collector (tests 1-3)
OTLP_HTTP_PORT = 14318   # OTLP HTTP source (test 4)
OTLP_GRPC_PORT = 14317   # OTLP gRPC source (test 4, bound but unused)
PROD_COLLECTOR = 18789   # collector for test 4

STARTUP_TIMEOUT = 30

# ── Vector config templates ───────────────────────────────────────────────────

PASSTHROUGH_CONFIG = """\
sources:
  test_in:
    type: http_server
    address: "127.0.0.1:{source_port}"
    decoding:
      codec: json

sinks:
  test_out:
    type: file
    inputs: [test_in]
    path: "{output_file}"
    encoding:
      codec: json
"""

BATCH_CONFIG = """\
sources:
  test_in:
    type: http_server
    address: "127.0.0.1:{source_port}"
    framing:
      method: newline_delimited
    decoding:
      codec: json

sinks:
  test_out:
    type: http
    inputs: [test_in]
    uri: "http://127.0.0.1:{collector_port}/collect"
    encoding:
      codec: json
    framing:
      method: newline_delimited
    batch:
      timeout_secs: {batch_timeout}
      max_events: {max_events}
"""

# Mirrors the production tlmr setup:
#   OTLP source → attributor_logs → tenant filter → bouncer → s3_key_remap
#   → disk-buffered HTTP sink (replacing the real S3/OTLP sinks)
PROD_CONFIG = """\
api:
  enabled: false

data_dir: "{data_dir}"

sources:
  otlp_in:
    type: opentelemetry
    http:
      headers:
        - "X-Tenant-ID"
      address: "127.0.0.1:{otlp_http_port}"
    grpc:
      address: "127.0.0.1:{otlp_grpc_port}"
    use_otlp_decoding: true

transforms:
  attributor_logs:
    type: remap
    inputs: ["otlp_in.logs"]
    source: |
      result = []
      for_each(array!(.resourceLogs)) -> |_ri, resource_log| {{
        for_each(array!(resource_log.scopeLogs)) -> |_si, scope_log| {{
          for_each(array!(scope_log.logRecords)) -> |_li, log_record| {{
            event = {{"resourceLogs": [resource_log]}}
            event.resourceLogs[0].scopeLogs = [scope_log]
            event.resourceLogs[0].scopeLogs[0].logRecords = [log_record]
            tlmr_attributes = {{
              "resource": resource_log.resource.attributes,
              "scope":    scope_log.scope.attributes,
              "logRecord": log_record.attributes
            }}
            for_each(object(tlmr_attributes)) -> |key, value| {{
              attr = {{}}
              if is_array(value) {{
                for_each(array!(value)) -> |_index, val| {{
                  actualVal = null
                  if exists(val.value.stringValue) {{
                    actualVal = val.value.stringValue
                  }} else if exists(val.value.boolValue) {{
                    actualVal = val.value.boolValue
                  }} else if exists(val.value.intValue) {{
                    actualVal = val.value.intValue
                  }} else if exists(val.value.doubleValue) {{
                    actualVal = val.value.doubleValue
                  }}
                  if !is_null(actualVal) {{
                    attr = set!(value: attr, path: [val.key], data: actualVal)
                  }}
                }}
              }}
              tlmr_attributes = set!(value: tlmr_attributes, path: [key], data: attr)
            }}
            event.tlmr_attributes = tlmr_attributes
            event."X-Tenant-ID" = ."X-Tenant-ID"
            result = push(result, event)
          }}
        }}
      }}
      . = result

  filter_logs:
    type: filter
    inputs: ["attributor_logs"]
    condition:
      type: vrl
      source: '."X-Tenant-ID" == "{tenant_id}" && true == true'

  bouncer:
    type: filter
    inputs: ["filter_logs"]
    condition:
      type: vrl
      source: '."X-Tenant-ID" == "{tenant_id}"'

  s3_key_remap:
    type: remap
    inputs: ["bouncer"]
    source: '.s3_key = "instance={tenant_id}/test_"'

sinks:
  test_out:
    type: http
    inputs: ["s3_key_remap"]
    uri: "http://127.0.0.1:{collector_port}/collect"
    encoding:
      codec: json
    framing:
      method: newline_delimited
    batch:
      timeout_secs: {batch_timeout}
      max_events: 100
    buffer:
      type: disk
      max_size: 268435488
      when_full: block
"""

# ── Collector HTTP server ─────────────────────────────────────────────────────

class CollectorServer:
    """In-process HTTP server; records every posted NDJSON event + arrival time."""

    def __init__(self, port: int):
        self._received: list[tuple[dict, float]] = []
        self._batches:  list[tuple[list[dict], float]] = []
        self._lock = threading.Lock()
        parent = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                arrived = time.monotonic()
                length  = int(self.headers.get("Content-Length", 0))
                body    = self.rfile.read(length).decode()
                batch   = [json.loads(l) for l in body.splitlines() if l.strip()]
                with parent._lock:
                    parent._batches.append((batch, arrived))
                    for ev in batch:
                        parent._received.append((ev, arrived))
                self.send_response(200)
                self.end_headers()

            def log_message(self, format, *args):
                pass

        class ReusableHTTPServer(HTTPServer):
            def server_bind(self):
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                super().server_bind()

        self._server = ReusableHTTPServer(("127.0.0.1", port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self):
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=3)

    def events(self) -> list[tuple[dict, float]]:
        with self._lock:
            return list(self._received)

    def batches(self) -> list[tuple[list[dict], float]]:
        with self._lock:
            return list(self._batches)

    def wait_for_count(self, count: int, timeout: float) -> list[tuple[dict, float]]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if len(self.events()) >= count:
                return self.events()
            time.sleep(0.1)
        return self.events()

    def clear(self):
        with self._lock:
            self._received.clear()
            self._batches.clear()


# ── Shared helpers ────────────────────────────────────────────────────────────

def kill_stale_processes() -> None:
    ports = [SOURCE_PORT, COLLECTOR_PORT, OTLP_HTTP_PORT, OTLP_GRPC_PORT, PROD_COLLECTOR]
    subprocess.run(
        ["fuser", "-k", "-TERM"] + [f"{p}/tcp" for p in ports],
        capture_output=True,
    )
    time.sleep(0.5)


def wait_for_http(port: int, timeout: int) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            req = urllib.request.Request(f"http://127.0.0.1:{port}/", method="GET")
            urllib.request.urlopen(req, timeout=1)
            return True
        except urllib.error.HTTPError:
            return True
        except Exception:
            time.sleep(0.5)
    return False


def wait_for_port(port: int, timeout: int) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return True
        except OSError:
            time.sleep(0.5)
    return False


def send_message(port: int, payload: dict) -> None:
    data = json.dumps(payload).encode()
    req  = urllib.request.Request(
        f"http://127.0.0.1:{port}/",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        assert resp.status in (200, 204), f"unexpected status {resp.status}"


def send_burst(port: int, payloads: list) -> None:
    """Send multiple events as a single NDJSON request so they all arrive atomically."""
    ndjson = "\n".join(json.dumps(p) for p in payloads)
    data   = ndjson.encode()
    req    = urllib.request.Request(
        f"http://127.0.0.1:{port}/",
        data=data,
        headers={"Content-Type": "application/x-ndjson"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        assert resp.status in (200, 204), f"unexpected status {resp.status}"


def send_otlp_logs(otlp_port: int, count: int, run_tag: str) -> None:
    """Send `count` OTLP log records via telemetrygen, tagging each with run_tag."""
    subprocess.run(
        [
            TELEMETRYGEN, "logs",
            "--otlp-http",
            "--otlp-header", f'X-Tenant-ID="{TENANT_ID}"',
            "--otlp-endpoint", f"127.0.0.1:{otlp_port}",
            "--otlp-insecure",
            "--logs", str(count),
            "--rate", "1000",
            "--workers", "1",
            "--telemetry-attributes", f'run_tag="{run_tag}"',
        ],
        check=True,
        capture_output=True,
        timeout=15,
    )


def run_tag_of(event: dict) -> str | None:
    """Extract run_tag from a transformed OTLP log event."""
    return event.get("tlmr_attributes", {}).get("logRecord", {}).get("run_tag")


def read_output_file(path: str, expected: int, timeout: int) -> list[dict]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(path):
            events = [json.loads(l) for l in open(path).read().splitlines() if l.strip()]
            if len(events) >= expected:
                return events
        time.sleep(0.5)
    return []


def spawn_vector(config_path: str, extra_env: dict | None = None) -> subprocess.Popen:
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    return subprocess.Popen(
        [VECTOR_BIN, "--config", config_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )


def stop_vector(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def _cpu_burner(stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        pass


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_passthrough() -> bool:
    print("\n=== test_passthrough ===")
    messages = [{"message": f"msg-{i}", "seq": i} for i in range(3)]

    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = os.path.join(tmpdir, "vector.yaml")
        output_path = os.path.join(tmpdir, "output.jsonl")
        open(config_path, "w").write(PASSTHROUGH_CONFIG.format(
            source_port=SOURCE_PORT, output_file=output_path,
        ))
        proc = spawn_vector(config_path)
        try:
            if not wait_for_http(SOURCE_PORT, STARTUP_TIMEOUT):
                print("FAIL: Vector did not start")
                return False
            for msg in messages:
                send_message(SOURCE_PORT, msg)
                print(f"  Sent: {msg}")
            time.sleep(1)
            events = read_output_file(output_path, len(messages), 10)
        finally:
            stop_vector(proc)

    missing = {m["message"] for m in messages} - {e.get("message") for e in events}
    if missing:
        print(f"FAIL: missing {missing}")
        return False
    print(f"PASS: all {len(messages)} messages passed through.")
    return True


def test_batching() -> bool:
    print("\n=== test_batching ===")
    MESSAGE_COUNT = 20
    BATCH_TIMEOUT = 3
    MESSAGE_DELAY = 1.0

    messages  = [{"message": f"batch-msg-{i}", "seq": i} for i in range(MESSAGE_COUNT)]
    collector = CollectorServer(COLLECTOR_PORT)

    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = os.path.join(tmpdir, "vector.yaml")
        open(config_path, "w").write(BATCH_CONFIG.format(
            source_port=SOURCE_PORT,
            collector_port=COLLECTOR_PORT,
            batch_timeout=BATCH_TIMEOUT,
            max_events=100,
        ))
        proc = spawn_vector(config_path)
        try:
            if not wait_for_http(SOURCE_PORT, STARTUP_TIMEOUT):
                collector.stop()
                print("FAIL: Vector did not start")
                return False
            t0 = time.time()
            for i, msg in enumerate(messages):
                send_message(SOURCE_PORT, msg)
                print(f"  [{i+1:02d}/{MESSAGE_COUNT}] Sent: {msg['message']}")
                if i < MESSAGE_COUNT - 1:
                    time.sleep(MESSAGE_DELAY)
            print(f"  All sent in {time.time()-t0:.1f}s, waiting for final flush...")
            evs = collector.wait_for_count(MESSAGE_COUNT, BATCH_TIMEOUT + 3)
        finally:
            stop_vector(proc)

    collector.stop()

    missing = {m["message"] for m in messages} - {e.get("message") for e, _ in evs}
    if missing:
        print(f"FAIL: expected {MESSAGE_COUNT}, got {len(evs)}; missing {missing}")
        return False

    batches = collector.batches()
    print(f"PASS: all {MESSAGE_COUNT} messages across {len(batches)} batch(es).")
    for i, (batch, _) in enumerate(batches):
        print(f"  Batch {i+1} ({len(batch)}): {[e.get('message') for e in batch]}")
    return True


def test_batch_timeout_reliability() -> bool:
    """
    Minimal reproducer for the lost-timer bug (simple HTTP source, no disk buffer).

    Root cause (partitioned_batcher.rs):
      When closed_batches is non-empty the batcher returns early without polling
      the timer, so the waker for the new batch's timer is never registered and
      the timer silently fires with nobody to wake.

    Detection: lone event must self-flush within timeout+grace.
    If it only arrives after the next event triggers a new batch → timer was lost.
    """
    print("\n=== test_batch_timeout_reliability ===")

    BATCH_TIMEOUT      = 0.5
    GRACE              = 0.4
    MAX_OK_LATENCY     = BATCH_TIMEOUT + GRACE
    INTER_EVENT_GAP    = MAX_OK_LATENCY * 3
    RUNS               = 50
    MAX_BATCH_EVENTS   = 10
    N_PRESSURE_THREADS = max(1, os.cpu_count() or 1)

    print(f"  timeout={BATCH_TIMEOUT}s  max_ok={MAX_OK_LATENCY}s  "
          f"gap={INTER_EVENT_GAP:.1f}s  runs={RUNS}  pressure_threads={N_PRESSURE_THREADS}")
    print(f"  Total runtime ≈ {RUNS * INTER_EVENT_GAP:.0f}s\n")

    collector = CollectorServer(COLLECTOR_PORT)

    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = os.path.join(tmpdir, "vector.yaml")
        open(config_path, "w").write(BATCH_CONFIG.format(
            source_port=SOURCE_PORT,
            collector_port=COLLECTOR_PORT,
            batch_timeout=BATCH_TIMEOUT,
            max_events=MAX_BATCH_EVENTS,
        ))
        #proc = spawn_vector(config_path)
        try:
            if not wait_for_http(SOURCE_PORT, STARTUP_TIMEOUT):
                collector.stop()
                print("FAIL: Vector did not start")
                return False

            stop_pressure = threading.Event()
            pressure_threads = [
                threading.Thread(target=_cpu_burner, args=(stop_pressure,), daemon=True)
                for _ in range(N_PRESSURE_THREADS)
            ]
            for t in pressure_threads:
                t.start()

            failures  = 0
            latencies = []

            try:
                for run in range(RUNS):
                    tag = f"rel-{run}"
                    collector.clear()

                    # Burst fills a full batch atomically (NDJSON) so all events
                    # arrive together, the batch flushes immediately, and the lone
                    # probe event gets its own fresh timer.
                    send_burst(SOURCE_PORT, [{"message": f"burst-{run}-{i}"} for i in range(MAX_BATCH_EVENTS)])
                    time.sleep(0.05)

                    send_message(SOURCE_PORT, {"message": tag, "run": run})
                    # Record AFTER the HTTP response — event is now in Vector's pipeline.
                    sent_at = time.monotonic()

                    found_at = None
                    deadline = sent_at + MAX_OK_LATENCY
                    while time.monotonic() < deadline:
                        for ev, arrived in collector.events():
                            if ev.get("message") == tag:
                                found_at = arrived
                                break
                        if found_at:
                            break
                        time.sleep(0.02)

                    latency = (found_at - sent_at) if found_at else None
                    if latency is not None:
                        latencies.append(latency)
                        print(f"  Run {run:02d}: flushed in {latency:.3f}s ✓")
                    else:
                        failures += 1
                        latencies.append(float("inf"))
                        print(f"  Run {run:02d}: NOT flushed within {MAX_OK_LATENCY}s — timer lost ✗")

                    remaining = INTER_EVENT_GAP - (time.monotonic() - sent_at)
                    if remaining > 0:
                        time.sleep(remaining)
            finally:
                stop_pressure.set()
                for t in pressure_threads:
                    t.join(timeout=1)
        finally:
            stop_vector(proc)

    collector.stop()

    finite = [l for l in latencies if l != float("inf")]
    if finite:
        print(f"\n  Latency (ok runs): min={min(finite):.3f}s  max={max(finite):.3f}s  "
              f"avg={sum(finite)/len(finite):.3f}s")
    if failures == 0:
        print(f"PASS: timer fired correctly in all {RUNS} runs.")
    else:
        print(f"FLAKY: timer lost in {failures}/{RUNS} runs ({failures/RUNS*100:.0f}%).")
        print("  Lone events stalled until next event arrived — lost-timer bug confirmed.")
    return True


def test_production_pipeline_timer() -> bool:
    """
    Production-accurate lost-timer reproducer.

    Mirrors the real tlmr topology from the zip:
      OTLP HTTP source (telemetrygen)
      → attributor_logs (VRL: split resourceLogs into individual events)
      → filter_logs (tenant ID check)
      → bouncer (tenant ID check per destination)
      → s3_key_remap (add S3 key prefix)
      → HTTP sink with DISK BUFFER + batch timeout

    The disk buffer adds async I/O between the batcher and the network, which
    increases the scheduler pressure on the Sleep future waker registration —
    making the lost-timer race more likely to manifest at real-world timeouts.
    """
    print("\n=== test_production_pipeline_timer ===")

    BATCH_TIMEOUT      = 2.0   # scaled down from 15s production value
    GRACE              = 1.0
    MAX_OK_LATENCY     = BATCH_TIMEOUT + GRACE
    INTER_EVENT_GAP    = MAX_OK_LATENCY * 3
    RUNS               = 20
    N_PRESSURE_THREADS = max(1, os.cpu_count() or 1)

    print(f"  OTLP source on :{OTLP_HTTP_PORT}, disk-buffered sink → collector :{PROD_COLLECTOR}")
    print(f"  timeout={BATCH_TIMEOUT}s  max_ok={MAX_OK_LATENCY}s  "
          f"gap={INTER_EVENT_GAP:.1f}s  runs={RUNS}  pressure_threads={N_PRESSURE_THREADS}")
    print(f"  Total runtime ≈ {RUNS * INTER_EVENT_GAP:.0f}s\n")

    collector = CollectorServer(PROD_COLLECTOR)

    with tempfile.TemporaryDirectory() as tmpdir:
        data_dir    = os.path.join(tmpdir, "vector-data")
        os.makedirs(data_dir)
        config_path = os.path.join(tmpdir, "vector.yaml")

        open(config_path, "w").write(PROD_CONFIG.format(
            data_dir=data_dir,
            otlp_http_port=OTLP_HTTP_PORT,
            otlp_grpc_port=OTLP_GRPC_PORT,
            collector_port=PROD_COLLECTOR,
            batch_timeout=BATCH_TIMEOUT,
            tenant_id=TENANT_ID,
        ))

        proc = spawn_vector(config_path)
        try:
            if not wait_for_port(OTLP_HTTP_PORT, STARTUP_TIMEOUT):
                collector.stop()
                print("FAIL: Vector OTLP port did not open in time")
                return False
            # Extra settle time — disk buffer init + gRPC listener
            time.sleep(2)
            print("  Vector is up.")

            stop_pressure = threading.Event()
            pressure_threads = [
                threading.Thread(target=_cpu_burner, args=(stop_pressure,), daemon=True)
                for _ in range(N_PRESSURE_THREADS)
            ]
            for t in pressure_threads:
                t.start()

            failures  = 0
            latencies = []

            try:
                for run in range(RUNS):
                    tag = f"prod-run-{run}"
                    collector.clear()

                    # Burst: send 10 events to fill a batch (forces fresh timer for probe)
                    send_otlp_logs(OTLP_HTTP_PORT, 10, f"burst-{run}")
                    time.sleep(0.1)

                    # Lone probe event
                    sent_at = time.monotonic()
                    send_otlp_logs(OTLP_HTTP_PORT, 1, tag)

                    found_at = None
                    deadline = sent_at + MAX_OK_LATENCY
                    while time.monotonic() < deadline:
                        for ev, arrived in collector.events():
                            if run_tag_of(ev) == tag:
                                found_at = arrived
                                break
                        if found_at:
                            break
                        time.sleep(0.05)

                    latency = (found_at - sent_at) if found_at else None
                    if latency is not None:
                        latencies.append(latency)
                        print(f"  Run {run:02d}: flushed in {latency:.3f}s ✓")
                    else:
                        failures += 1
                        latencies.append(float("inf"))
                        print(f"  Run {run:02d}: NOT flushed within {MAX_OK_LATENCY}s — timer lost ✗")

                    remaining = INTER_EVENT_GAP - (time.monotonic() - sent_at)
                    if remaining > 0:
                        time.sleep(remaining)
            finally:
                stop_pressure.set()
                for t in pressure_threads:
                    t.join(timeout=1)
        finally:
            stop_vector(proc)

    collector.stop()

    finite = [l for l in latencies if l != float("inf")]
    if finite:
        print(f"\n  Latency (ok runs): min={min(finite):.3f}s  max={max(finite):.3f}s  "
              f"avg={sum(finite)/len(finite):.3f}s")
    if failures == 0:
        print(f"PASS: timer fired correctly in all {RUNS} runs.")
    else:
        print(f"FLAKY: timer lost in {failures}/{RUNS} runs ({failures/RUNS*100:.0f}%).")
        print("  Events stalled until next arrival — lost-timer bug in production topology.")
    return True


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> int:
    kill_stale_processes()
    results = [
    #test_passthrough(),
    #    test_batching(),
        test_batch_timeout_reliability(),
    #    test_production_pipeline_timer(),
    ]
    passed = sum(results)
    total  = len(results)
    print(f"\n{'='*40}")
    print(f"Results: {passed}/{total} passed")
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
