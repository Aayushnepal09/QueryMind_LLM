"""Tracing tests.

The contract this module has to keep is negative: observability must never
break the thing it observes. So most of these assert that things *do not*
happen — no raise when the SDK is missing, no raise when an exporter is
misconfigured, no raise when a span attribute is unset.
"""

from __future__ import annotations

import sqlsentinel.tracing as tracing_module
from sqlsentinel.tracing import get_tracer, record_agent_trace, span


def _reset(monkeypatch):
    """Clear the module-level memoisation between cases."""
    monkeypatch.setattr(tracing_module, "_TRACER", None)
    monkeypatch.setattr(tracing_module, "_INITIALISED", False)


class FakeTrace:
    db_id = "shop"
    cost_usd = 0.01
    latency_s = 1.5
    prompt_tokens = 100
    output_tokens = 20
    n_candidates = 3
    agreement_rate = 0.67
    n_correction_rounds = 1
    executed_ok = True


# ---------------------------------------------------------------- disabled


def test_tracing_can_be_switched_off(monkeypatch):
    _reset(monkeypatch)
    monkeypatch.setenv("SQLSENTINEL_TRACING", "0")
    assert get_tracer() is None


def test_span_is_a_no_op_without_a_tracer():
    with span(None, "anything", trace_id="abc") as s:
        assert s is None


def test_record_agent_trace_without_a_tracer_does_nothing():
    record_agent_trace(None, FakeTrace())  # must not raise


# ---------------------------------------------------------------- memoisation


def test_tracer_is_built_once(monkeypatch):
    _reset(monkeypatch)
    monkeypatch.setenv("SQLSENTINEL_TRACING", "0")
    first = get_tracer()
    monkeypatch.setenv("SQLSENTINEL_TRACING", "1")
    # already initialised, so the env change must not take effect mid-process
    assert get_tracer() is first


# ---------------------------------------------------------------- failure paths


def test_a_broken_sdk_degrades_to_none_rather_than_raising(monkeypatch):
    """An eval run must not fail because a tracing backend is unavailable."""
    _reset(monkeypatch)
    monkeypatch.setenv("SQLSENTINEL_TRACING", "1")

    import builtins

    real_import = builtins.__import__

    def explode(name, *a, **kw):
        if name.startswith("opentelemetry"):
            raise ImportError("simulated missing SDK")
        return real_import(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", explode)
    assert get_tracer() is None


# Deliberately not tested here: pointing OTEL_EXPORTER_OTLP_ENDPOINT at a dead
# host. It exercises the same "must not raise" path as the case above, but
# leaves a BatchSpanProcessor retrying on a background thread and printing
# connection errors into every subsequent test run. Verified manually instead.


# ---------------------------------------------------------------- enabled path


def test_spans_and_attributes_work_when_enabled(monkeypatch):
    _reset(monkeypatch)
    monkeypatch.setenv("SQLSENTINEL_TRACING", "1")
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)

    tracer = get_tracer("test-service")
    if tracer is None:  # SDK genuinely absent
        return

    with span(tracer, "query", trace_id="abc", db_id="shop", skipped=None) as s:
        assert s is not None
        record_agent_trace(tracer, FakeTrace())


def test_none_attributes_are_skipped(monkeypatch):
    """A span attribute set to None would raise in the OTel SDK."""
    _reset(monkeypatch)
    monkeypatch.setenv("SQLSENTINEL_TRACING", "1")
    tracer = get_tracer("test-service")
    if tracer is None:
        return
    with span(tracer, "x", present="yes", absent=None):
        pass
