import os

import pytest

from sqlsentinel.llm import (
    GeminiClient,
    LLMResponse,
    OllamaClient,
    ResponseCache,
    _BaseClient,
    get_client,
)


@pytest.fixture
def cache(tmp_path):
    return ResponseCache(tmp_path / "c.db")


class FakeClient(_BaseClient):
    """Counts real calls so cache hits are observable."""

    provider = "fake"

    def __init__(self, cache):
        super().__init__("fake-model", cache)
        self.calls = 0

    def _call(self, system, user, temperature, max_tokens):
        self.calls += 1
        return LLMResponse(
            text=f"resp-{self.calls}",
            model=self.model,
            provider=self.provider,
            prompt_tokens=10,
            output_tokens=5,
        )


def test_cache_prevents_second_call(cache):
    c = FakeClient(cache)
    a = c.complete("sys", "user")
    b = c.complete("sys", "user")
    assert c.calls == 1
    assert a.text == b.text
    assert not a.cached and b.cached


def test_sample_index_is_part_of_the_key(cache):
    """k-sampling must not collapse into one cached entry."""
    c = FakeClient(cache)
    c.complete("sys", "user", temperature=0.7, sample_index=0)
    c.complete("sys", "user", temperature=0.7, sample_index=1)
    assert c.calls == 2


@pytest.mark.parametrize(
    "kwargs",
    [
        {"system": "other"},
        {"user": "other"},
        {"temperature": 0.7},
        {"sample_index": 3},
    ],
)
def test_every_key_component_changes_the_key(kwargs):
    base = {"system": "sys", "user": "user", "temperature": 0.0, "sample_index": 0}
    k1 = ResponseCache.key("p", "m", **base)
    k2 = ResponseCache.key("p", "m", **{**base, **kwargs})
    assert k1 != k2


def test_cache_persists_across_instances(tmp_path):
    path = tmp_path / "c.db"
    c1 = FakeClient(ResponseCache(path))
    c1.complete("sys", "user")
    c2 = FakeClient(ResponseCache(path))
    hit = c2.complete("sys", "user")
    assert c2.calls == 0 and hit.cached


def test_local_model_costs_nothing():
    r = LLMResponse("x", "qwen2.5-coder:7b", "ollama", 1_000_000, 1_000_000)
    assert r.cost_usd == 0.0


def test_hosted_cost_uses_price_table():
    r = LLMResponse("x", "gemini-2.5-flash", "gemini", 1_000_000, 1_000_000)
    assert r.cost_usd == pytest.approx(0.30 + 2.50)


def test_unknown_model_costs_zero_rather_than_crashing():
    assert LLMResponse("x", "nope", "gemini", 1000, 1000).cost_usd == 0.0


def test_get_client_dispatch(monkeypatch, cache):
    monkeypatch.setenv("SQLSENTINEL_PROVIDER", "ollama")
    assert isinstance(get_client(cache=cache), OllamaClient)
    with pytest.raises(ValueError, match="unknown provider"):
        get_client("anthropic", cache=cache)


def test_gemini_without_key_gives_actionable_error(monkeypatch, cache):
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match=r"aistudio\.google\.com"):
        GeminiClient(cache=cache)


@pytest.mark.skipif(os.getenv("SQLSENTINEL_SKIP_LIVE") == "1", reason="live model call disabled")
def test_ollama_live_roundtrip(cache):
    """Hits the real local model. Skipped in CI, run locally."""
    pytest.importorskip("ollama")
    try:
        r = OllamaClient(cache=cache).complete(
            "Output only SQL, no prose.", "Table t(a int). Count the rows.", max_tokens=64
        )
    except Exception as e:  # ollama not running
        pytest.skip(f"ollama unavailable: {e}")
    assert "select" in r.text.lower()
    assert r.output_tokens > 0
    assert r.provider == "ollama"


# ---------------------------------------------------------------- gemini retry


class Boom(Exception):
    """Stands in for the SDK's 429."""


def test_retry_delay_parsing():
    from sqlsentinel.llm import _retry_delay

    assert _retry_delay("{'retryDelay': '31s'}") == 32.0
    assert _retry_delay('retryDelay: "7s"') == 8.0
    assert _retry_delay("no delay here") is None


def test_gemini_retries_on_429_then_succeeds(monkeypatch, cache):
    """A rate limit must slow a run down, not fail it.

    The free tier is reached after only a handful of BIRD-sized prompts, so an
    unretried run loses most of its questions rather than merely taking longer.
    """
    import sqlsentinel.llm as llm_module

    monkeypatch.setenv("GEMINI_API_KEY", "test-key")
    monkeypatch.setattr(llm_module.time, "sleep", lambda _s: None)

    client = GeminiClient(cache=cache)
    calls = {"n": 0}

    def flaky(system, user, temperature, max_tokens):
        calls["n"] += 1
        if calls["n"] < 3:
            raise Boom("429 RESOURCE_EXHAUSTED {'retryDelay': '1s'}")
        return LLMResponse(text="SELECT 1", model=client.model, provider="gemini")

    monkeypatch.setattr(client, "_generate", flaky)
    assert client.complete("s", "u").text == "SELECT 1"
    assert calls["n"] == 3


def test_gemini_gives_up_after_the_retry_budget(monkeypatch, cache):
    import sqlsentinel.llm as llm_module

    monkeypatch.setenv("GEMINI_API_KEY", "test-key")
    monkeypatch.setattr(llm_module.time, "sleep", lambda _s: None)

    client = GeminiClient(cache=cache)

    def always_limited(*_args):
        raise Boom("429 RESOURCE_EXHAUSTED")

    monkeypatch.setattr(client, "_generate", always_limited)
    with pytest.raises(RuntimeError, match="rate limited"):
        client.complete("s", "u")


def test_gemini_does_not_retry_other_errors(monkeypatch, cache):
    """Only rate limits are transient; a bad request must surface immediately."""
    monkeypatch.setenv("GEMINI_API_KEY", "test-key")
    client = GeminiClient(cache=cache)
    calls = {"n": 0}

    def bad_request(*_args):
        calls["n"] += 1
        raise Boom("400 INVALID_ARGUMENT")

    monkeypatch.setattr(client, "_generate", bad_request)
    with pytest.raises(Boom):
        client.complete("s", "u")
    assert calls["n"] == 1


def test_cache_stats_counts_entries(cache):
    c = FakeClient(cache)
    assert cache.stats()["entries"] == 0
    c.complete("sys", "user")
    assert cache.stats()["entries"] == 1
