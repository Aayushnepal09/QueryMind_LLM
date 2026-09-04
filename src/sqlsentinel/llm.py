"""Provider-agnostic LLM interface with an on-disk response cache.

CLAUDE.md section 5 wants one thin abstraction over at least two providers so
they can be compared (section 13: pick on cost-per-correct-answer, not raw
accuracy). Both providers here are free:

    ollama  -- qwen2.5-coder:7b running locally. No key, no quota, ~10 tok/s
               on the target GPU. The workhorse for k-sampling.
    gemini  -- gemini-2.5-flash on the free tier. Fast, rate-limited, needs a
               key. Used for phase-boundary runs and provider comparison.

The cache is not an optimisation, it is the budget. At ~10 tok/s locally a
500-question run at k=5 is hours of wall-clock; without caching every re-run of
an unchanged config pays that again. Keyed on
(provider, model, system, user, temperature, sample_index) exactly as
CLAUDE.md section 8 specifies -- sample_index is what keeps the k samples of a
self-consistency draw distinct instead of collapsing into one cached entry.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

REPO_ROOT = Path(__file__).resolve().parents[2]

# USD per 1M tokens (input, output). Both providers are used on free tiers so
# actual spend is zero; these let us report what the same run *would* cost on
# paid infrastructure, which is the comparison a reviewer cares about.
PRICING: dict[str, tuple[float, float]] = {
    "gemini-2.5-flash": (0.30, 2.50),
    "gemini-2.0-flash": (0.10, 0.40),
}

MAX_RETRIES = 6


def _retry_delay(message: str) -> float | None:
    """Pull the server's suggested retryDelay (e.g. "retryDelay': '31s'") out of
    a 429 payload, so we wait exactly as long as asked rather than guessing."""
    m = re.search(r"retryDelay['\"]?\s*:\s*['\"]?(\d+(?:\.\d+)?)s", message)
    return float(m.group(1)) + 1.0 if m else None


@dataclass
class LLMResponse:
    text: str
    model: str
    provider: str
    prompt_tokens: int = 0
    output_tokens: int = 0
    latency_s: float = 0.0
    cached: bool = False

    @property
    def cost_usd(self) -> float:
        """Nominal cost. Zero for local models; list price for hosted ones."""
        if self.provider == "ollama":
            return 0.0
        rate_in, rate_out = PRICING.get(self.model, (0.0, 0.0))
        return (self.prompt_tokens * rate_in + self.output_tokens * rate_out) / 1e6


class ResponseCache:
    """SQLite-backed cache. Survives restarts; one connection per thread."""

    def __init__(self, path: Path | None = None):
        self.path = Path(path or REPO_ROOT / ".llm_cache" / "responses.db")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        conn = self._conn()
        conn.execute(
            "CREATE TABLE IF NOT EXISTS responses ("
            " key TEXT PRIMARY KEY, payload TEXT NOT NULL, created_at REAL NOT NULL)"
        )
        conn.commit()

    def _conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn"):
            self._local.conn = sqlite3.connect(self.path, timeout=30)
            self._local.conn.execute("PRAGMA journal_mode=WAL")
        return self._local.conn

    @staticmethod
    def key(
        provider: str,
        model: str,
        system: str,
        user: str,
        temperature: float,
        sample_index: int,
    ) -> str:
        blob = json.dumps(
            [provider, model, system, user, round(temperature, 4), sample_index],
            sort_keys=True,
        )
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    def get(self, key: str) -> LLMResponse | None:
        row = self._conn().execute(
            "SELECT payload FROM responses WHERE key = ?", (key,)
        ).fetchone()
        if row is None:
            return None
        return LLMResponse(**json.loads(row[0]), cached=True)

    def put(self, key: str, resp: LLMResponse) -> None:
        payload = {
            "text": resp.text,
            "model": resp.model,
            "provider": resp.provider,
            "prompt_tokens": resp.prompt_tokens,
            "output_tokens": resp.output_tokens,
            "latency_s": resp.latency_s,
        }
        conn = self._conn()
        conn.execute(
            "INSERT OR REPLACE INTO responses VALUES (?, ?, ?)",
            (key, json.dumps(payload), time.time()),
        )
        conn.commit()

    def stats(self) -> dict[str, int]:
        return {"entries": self._conn().execute("SELECT COUNT(*) FROM responses").fetchone()[0]}


class LLMClient(Protocol):
    provider: str
    model: str

    def complete(
        self,
        system: str,
        user: str,
        *,
        temperature: float = 0.0,
        sample_index: int = 0,
        max_tokens: int = 512,
    ) -> LLMResponse: ...


class _BaseClient:
    provider = "base"

    def __init__(self, model: str, cache: ResponseCache | None = None):
        self.model = model
        self.cache = cache if cache is not None else ResponseCache()

    def complete(
        self,
        system: str,
        user: str,
        *,
        temperature: float = 0.0,
        sample_index: int = 0,
        max_tokens: int = 512,
    ) -> LLMResponse:
        key = ResponseCache.key(
            self.provider, self.model, system, user, temperature, sample_index
        )
        hit = self.cache.get(key)
        if hit is not None:
            return hit

        t0 = time.time()
        resp = self._call(system, user, temperature, max_tokens)
        resp.latency_s = time.time() - t0
        self.cache.put(key, resp)
        return resp

    def _call(
        self, system: str, user: str, temperature: float, max_tokens: int
    ) -> LLMResponse:
        raise NotImplementedError


class OllamaClient(_BaseClient):
    provider = "ollama"

    def __init__(
        self,
        model: str | None = None,
        host: str | None = None,
        cache: ResponseCache | None = None,
    ):
        super().__init__(model or os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b"), cache)
        self.host = host or os.getenv("OLLAMA_HOST", "http://localhost:11434")

    def _call(self, system, user, temperature, max_tokens) -> LLMResponse:
        import ollama

        r = ollama.Client(host=self.host).chat(
            model=self.model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            options={"temperature": temperature, "num_predict": max_tokens},
        )
        return LLMResponse(
            text=r["message"]["content"],
            model=self.model,
            provider=self.provider,
            prompt_tokens=r.get("prompt_eval_count", 0) or 0,
            output_tokens=r.get("eval_count", 0) or 0,
        )


class GeminiClient(_BaseClient):
    provider = "gemini"

    def __init__(
        self,
        model: str | None = None,
        api_key: str | None = None,
        cache: ResponseCache | None = None,
    ):
        super().__init__(model or os.getenv("GEMINI_MODEL", "gemini-2.5-flash"), cache)
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise RuntimeError(
                "GEMINI_API_KEY is not set. Get a free key at "
                "https://aistudio.google.com/apikey and put it in .env"
            )
        self._client = None

    @property
    def client(self):
        """Built once and held.

        Constructing the client inline as a temporary lets it be closed before
        the response is consumed, which surfaces as a confusing
        "client has been closed" RuntimeError.
        """
        if self._client is None:
            from google import genai

            self._client = genai.Client(api_key=self.api_key)
        return self._client

    def _call(self, system, user, temperature, max_tokens) -> LLMResponse:
        """Retries on 429.

        The free tier's per-minute limit is reached after only a handful of
        BIRD-sized prompts (~1,400 prompt tokens each), so an unretried eval run
        fails most of its questions rather than merely running slowly. Honours
        the server's retryDelay when it supplies one, otherwise backs off
        exponentially.
        """
        last: Exception | None = None
        for attempt in range(MAX_RETRIES):
            try:
                return self._generate(system, user, temperature, max_tokens)
            except Exception as e:
                last = e
                msg = str(e)
                if "429" not in msg and "RESOURCE_EXHAUSTED" not in msg:
                    raise
                if attempt == MAX_RETRIES - 1:
                    break
                wait = _retry_delay(msg) or min(2.0 * 2**attempt, 60.0)
                time.sleep(wait)
        raise RuntimeError(f"gemini rate limited after {MAX_RETRIES} attempts: {last}")

    def _generate(self, system, user, temperature, max_tokens) -> LLMResponse:
        from google.genai import types

        r = self.client.models.generate_content(
            model=self.model,
            contents=user,
            config=types.GenerateContentConfig(
                system_instruction=system,
                temperature=temperature,
                max_output_tokens=max_tokens,
            ),
        )
        usage = getattr(r, "usage_metadata", None)
        return LLMResponse(
            text=r.text or "",
            model=self.model,
            provider=self.provider,
            prompt_tokens=getattr(usage, "prompt_token_count", 0) or 0,
            output_tokens=getattr(usage, "candidates_token_count", 0) or 0,
        )


def get_client(
    provider: str | None = None, cache: ResponseCache | None = None
) -> LLMClient:
    """Build a client from SQLSENTINEL_PROVIDER, or an explicit override."""
    name = (provider or os.getenv("SQLSENTINEL_PROVIDER", "ollama")).lower()
    if name == "ollama":
        return OllamaClient(cache=cache)
    if name == "gemini":
        return GeminiClient(cache=cache)
    raise ValueError(f"unknown provider '{name}' (expected 'ollama' or 'gemini')")
