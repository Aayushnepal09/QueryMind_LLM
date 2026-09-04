"""OpenTelemetry tracing (CLAUDE.md section 4, `feat/observability`).

Spans for schema linking, each generation sample, each execution, scoring and
routing, carrying cost and latency per query.

Tracing is optional at runtime: if opentelemetry is not installed, or no
exporter is configured, `span()` degrades to a no-op context manager rather
than failing. An eval run must never break because a tracing backend is down.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any

_TRACER = None
_INITIALISED = False


def get_tracer(service_name: str = "sqlsentinel"):
    """Return a tracer, or None when tracing is unavailable or disabled."""
    global _TRACER, _INITIALISED
    if _INITIALISED:
        return _TRACER
    _INITIALISED = True

    if os.getenv("SQLSENTINEL_TRACING", "1") == "0":
        return None

    try:
        from opentelemetry import trace
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        provider = TracerProvider(
            resource=Resource.create({"service.name": service_name})
        )

        endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
        if endpoint:
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
                OTLPSpanExporter,
            )

            provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        elif os.getenv("SQLSENTINEL_TRACE_CONSOLE") == "1":
            from opentelemetry.sdk.trace.export import ConsoleSpanExporter

            provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

        trace.set_tracer_provider(provider)
        _TRACER = trace.get_tracer(service_name)
    except Exception:
        # Never let observability break the thing being observed.
        _TRACER = None
    return _TRACER


@contextmanager
def span(tracer, name: str, **attributes: Any):
    """Span context manager that no-ops when tracing is unavailable."""
    if tracer is None:
        yield None
        return
    with tracer.start_as_current_span(name) as s:
        for k, v in attributes.items():
            if v is not None:
                s.set_attribute(k, v)
        yield s


def record_agent_trace(tracer, trace) -> None:
    """Attach an AgentTrace's measurements to the current span."""
    if tracer is None:
        return
    try:
        from opentelemetry import trace as _t

        s = _t.get_current_span()
        for key, value in {
            "sqlsentinel.db_id": trace.db_id,
            "sqlsentinel.cost_usd": trace.cost_usd,
            "sqlsentinel.latency_s": trace.latency_s,
            "sqlsentinel.prompt_tokens": trace.prompt_tokens,
            "sqlsentinel.output_tokens": trace.output_tokens,
            "sqlsentinel.n_candidates": trace.n_candidates,
            "sqlsentinel.agreement_rate": trace.agreement_rate,
            "sqlsentinel.correction_rounds": trace.n_correction_rounds,
            "sqlsentinel.executed_ok": trace.executed_ok,
        }.items():
            s.set_attribute(key, value)
    except Exception:
        return
