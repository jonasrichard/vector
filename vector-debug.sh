#!/bin/bash

VECTOR_LOG="trace,vector::app=trace,vector::sinks::aws_s3=trace,vector::sinks::s3_common=trace,vector::topology::builder=trace"
VECTOR_INTERNAL_LOG_RATE_LIMIT=0
LOG_SENSITIVE_BODIES=true

target/debug/vector --config vector2.yaml
