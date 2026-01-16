"""Telemetry configuration."""

import logging

from opentelemetry import trace
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

logging.basicConfig(level=logging.INFO)
if not HTTPXClientInstrumentor().is_instrumented_by_opentelemetry:
    HTTPXClientInstrumentor().instrument()

tracer = trace.get_tracer("main.tracer")