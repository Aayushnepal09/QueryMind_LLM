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
def test_every_key_component_changes_the_key(cache, kwargs):
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
