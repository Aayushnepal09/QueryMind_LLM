# Reproducing the results

Fresh clone to a scored number. Everything here runs on free infrastructure —
no paid API, no GPU rental, no account beyond an optional free Gemini key.

## 1. Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/)
- [Ollama](https://ollama.com) for the local model path (optional if you only
  use Gemini)
- ~2 GB free disk for the benchmark

## 2. Install

```bash
uv sync --extra dev
```

## 3. Get the benchmark

Download the BIRD dev set from <https://bird-bench.github.io/>. **Pin the
version.** This project used the release whose archive unpacks to
`dev_20240627/` — the 2024-06-27 build, which predates BIRD's late-2025
re-annotation. A different release will produce different numbers, and they
will not be comparable to the ones in this repo.

```bash
mkdir -p data/bird
unzip dev.zip -d data/bird
unzip data/bird/dev_20240627/dev_databases.zip -d data/bird/dev_20240627
rm -rf data/bird/dev_20240627/__MACOSX data/bird/dev_20240627/dev_databases.zip
```

You should end with 11 `.sqlite` files:

```bash
find data/bird/dev_20240627/dev_databases -name '*.sqlite' | wc -l
```

## 4. Configure

```bash
cp .env.example .env
```

`BIRD_DEV_ROOT` already points at `data/bird/dev_20240627`. Choose a provider:

- **Local (no key, unlimited):** `ollama pull qwen2.5-coder:7b`, leave
  `SQLSENTINEL_PROVIDER=ollama`.
- **Gemini (free tier, rate-limited):** get a key at
  <https://aistudio.google.com/apikey>, set `GEMINI_API_KEY`, and set
  `SQLSENTINEL_PROVIDER=gemini`.

## 5. Verify the harness before trusting any number

The stub predictor emits `SELECT 1` for every question. It exercises the whole
path — split selection, prediction file, official scorer, parsing — with no
model involved.

```bash
uv run python -m sqlsentinel.eval --split dev_50 --predictor stub
```

Expect roughly 3–4%, **not** 0%. See `results/baseline-floor.md` for why the
chance floor is non-zero under set-equality scoring.

Then confirm the wrapper is not misaligning anything:

```bash
uv run pytest -q
```

`test_gold_sql_scores_100_percent` feeds BIRD's own gold SQL back through the
harness. If it scores anything below 100%, every downstream number is suspect.

## 6. Run the agent

```bash
uv run python -m sqlsentinel.eval --split dev_50 --predictor agent --workers 1
```

Add techniques individually to reproduce the ablation table:

```bash
uv run python -m sqlsentinel.eval --split dev_50 --predictor agent --few-shot 3 --tag fewshot3
uv run python -m sqlsentinel.eval --split dev_50 --predictor agent --prune-schema --tag pruned
uv run python -m sqlsentinel.eval --split dev_50 --predictor agent --max-corrections 2 --tag selfcorrect
```

The full reported run uses `--split eval_500` and `--dump-traces`:

```bash
uv run python -m sqlsentinel.eval --split eval_500 --predictor agent \
  --k 5 --dump-traces results/traces/final-eval500.json --tag final
```

## 7. Produce the report artifacts

```bash
uv run python scripts/analyze.py results/traces/final-eval500.json --label final \
  --calib-traces results/traces/final-calib.json
```

Writes calibration JSON + reliability diagram, the routing curve, and the
failure taxonomy into `results/`.

## 8. Run the service

```bash
docker compose up
```

- API: <http://localhost:8000/docs>
- Review UI: <http://localhost:8501>
- Traces: <http://localhost:6006>

`data/` mounts read-only, so the benchmark stays out of the image.

## Notes on cost and time

Nothing here costs money. It costs *time*, and the two providers trade
differently:

| | Local Qwen (GTX 1660 Ti) | Gemini free tier |
|---|---|---|
| Throughput | ~10 output tok/s | ~3 s/query, then rate-limited |
| Limit | none | per-minute quota; the client backs off and retries |
| 50 questions | ~20 min | a few minutes |
| 500 questions, k=1 | hours | ~1 hour with backoff |

**Response caching is what makes this tractable.** Every call is cached on
`(provider, model, system, user, temperature, sample_index)` in `.llm_cache/`,
so re-running an unchanged configuration is free and instant. Delete that
directory only if you intend to pay the wall-clock cost again.

## Windows note: Git Bash mangles container paths

Running an ad-hoc `docker run -v /app/data ...` from Git Bash silently rewrites
absolute container paths — `/app/data` becomes `C:/Program Files/Git/app/data`
— and the mount lands nowhere. The API still starts and reports healthy, but
with `"databases": 0`.

`docker compose up` is unaffected, because Compose reads the paths from YAML
rather than through the shell. If you do need a bare `docker run` on Windows,
disable the conversion:

```bash
MSYS_NO_PATHCONV=1 docker run -v "//c/path/to/repo/data:/app/data:ro" ...
```

Verified working configuration (2026-09-04): image builds clean, container
serves `/health` with all 11 databases visible, and `POST /query` returns
correct SQL executed against the mounted benchmark, reaching Ollama on the host
GPU via `host.docker.internal`.

## Fresh-clone verification (2026-09-04)

Cloned to an empty directory and installed from scratch:

| Check | Result |
|---|---|
| `uv sync --extra dev` | clean install |
| `uv run pytest` | **184 passed, 12 skipped** — the skips are the tests that need the BIRD databases, which self-skip when absent |
| `uv run ruff check` / `ruff format --check` | clean |
| `uv run sqlsentinel-eval --help` | works |
| Running the eval without BIRD present | fails with an actionable message pointing here |
| Secrets or benchmark data in the clone | none — `.env` and `data/` are absent as intended |

**Windows path-length caveat.** The first attempt failed with
`ModuleNotFoundError: No module named 'sklearn.metrics._pairwise_distances_reduction._radius_neighbors_classmode'`
— not a dependency problem. The clone sat under a deeply nested temp directory,
which pushed scikit-learn's compiled extension paths to 263 characters, past
Windows' 260-character `MAX_PATH`. The `.pyd` files were present and simply
could not be loaded. Clone to a short path, or enable long paths:

```
Set-ItemProperty 'HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem' LongPathsEnabled 1
```

## Re-recording the demo

```bash
uv run streamlit run app/review_ui.py --server.headless true --server.port 8610
uv run python scripts/record_demo.py
```

Writes `results/review-ui-demo.gif`. Scripted rather than hand-captured so it
can be regenerated after a UI change instead of going stale.
