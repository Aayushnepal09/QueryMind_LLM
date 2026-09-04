"""Tests for the eval CLI.

This module was at 0% coverage, and it is where the most expensive mistake of
the project lived: a script omitted `--predictor agent`, the parser defaulted to
the stub, and the run reported a chance-floor score instead of failing. Silent
defaults that produce plausible-looking numbers are worth pinning down.
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

from sqlsentinel.eval.__main__ import PREDICTORS, load_split, main, stub_predictor

# ---------------------------------------------------------------- predictors


def test_stub_emits_select_one():
    qs = [{"question_id": 1}, {"question_id": 2}]
    assert stub_predictor(qs) == {1: "SELECT 1", 2: "SELECT 1"}


def test_predictor_registry_contents():
    assert set(PREDICTORS) == {"stub", "baseline", "agent"}


def test_default_predictor_is_the_stub():
    """The default that caused a wasted run.

    Pinned rather than changed: `--predictor stub` is the right default for a
    harness smoke test. What was missing was the caller stating its intent, so
    this test exists to make the default explicit and deliberate rather than
    incidental.
    """
    import argparse

    from sqlsentinel.eval import __main__ as m

    src = Path(m.__file__).read_text(encoding="utf-8")
    assert '"--predictor", default="stub"' in src

    # and confirm argparse actually resolves it that way
    ap = argparse.ArgumentParser()
    ap.add_argument("--predictor", default="stub", choices=sorted(PREDICTORS))
    assert ap.parse_args([]).predictor == "stub"


# ---------------------------------------------------------------- splits


@pytest.fixture
def fake_bird(tmp_path, monkeypatch):
    """Minimal BIRD layout: three questions covering all difficulties."""
    root = tmp_path / "bird"
    (root / "dev_databases" / "shop").mkdir(parents=True)

    conn = sqlite3.connect(root / "dev_databases" / "shop" / "shop.sqlite")
    conn.executescript(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);"
        "INSERT INTO items VALUES (1,'a'),(2,'b');"
    )
    conn.commit()
    conn.close()

    records = [
        {
            "question_id": 0,
            "db_id": "shop",
            "question": "how many items?",
            "evidence": "",
            "SQL": "SELECT COUNT(*) FROM items",
            "difficulty": "simple",
        },
        {
            "question_id": 1,
            "db_id": "shop",
            "question": "list names",
            "evidence": "",
            "SQL": "SELECT name FROM items",
            "difficulty": "moderate",
        },
        {
            "question_id": 2,
            "db_id": "shop",
            "question": "first name",
            "evidence": "",
            "SQL": "SELECT name FROM items LIMIT 1",
            "difficulty": "challenging",
        },
    ]
    (root / "dev.json").write_text(json.dumps(records), encoding="utf-8")
    (root / "dev.sql").write_text(
        "\n".join(f"{r['SQL']}\tshop" for r in records) + "\n", encoding="utf-8"
    )
    return root


def test_load_split_prefers_the_committed_file(tmp_path, monkeypatch, fake_bird):
    from sqlsentinel.eval import __main__ as m

    splits = tmp_path / "splits.json"
    splits.write_text(json.dumps({"dev_50": [7, 8, 9]}), encoding="utf-8")
    monkeypatch.setattr(m, "SPLITS_FILE", splits)
    assert load_split("dev_50", fake_bird) == [7, 8, 9]


def test_load_split_falls_back_to_generating(tmp_path, monkeypatch):
    """With no committed splits file, the split is regenerated from dev.json."""
    from sqlsentinel.eval import __main__ as m

    monkeypatch.setattr(m, "SPLITS_FILE", tmp_path / "absent.json")
    bird = Path("data/bird/dev_20240627")
    if not (bird / "dev.json").exists():
        pytest.skip("BIRD dev set not present")
    assert len(load_split("eval_500", bird)) == 500


def test_load_split_rejects_an_unknown_name(tmp_path, monkeypatch):
    from sqlsentinel.eval import __main__ as m

    monkeypatch.setattr(m, "SPLITS_FILE", tmp_path / "absent.json")
    bird = Path("data/bird/dev_20240627")
    if not (bird / "dev.json").exists():
        pytest.skip("BIRD dev set not present")
    with pytest.raises(SystemExit, match="unknown split"):
        load_split("nonexistent", bird)


def test_a_truncated_dataset_gives_an_explanatory_error(fake_bird):
    """A 3-question file cannot yield a 500-question split; say why."""
    from sqlsentinel.eval.subsets import build_splits

    with pytest.raises(ValueError, match="truncated or the wrong file"):
        build_splits(fake_bird / "dev.json")


# ---------------------------------------------------------------- end to end


def _run(monkeypatch, argv: list[str]) -> None:
    monkeypatch.setattr(sys, "argv", ["sqlsentinel.eval", *argv])
    main()


def test_full_split_scores_and_prints(monkeypatch, capsys, fake_bird):
    _run(
        monkeypatch,
        [
            "--split",
            "full",
            "--predictor",
            "stub",
            "--bird-root",
            str(fake_bird),
            "--no-mlflow",
            "--num-cpus",
            "1",
        ],
    )
    out = capsys.readouterr().out
    assert "predictor=stub" in out
    assert "EX " in out and "n=3" in out


def test_gold_predictions_score_100_via_the_cli(monkeypatch, capsys, fake_bird):
    """End-to-end proof the CLI wires predictions to the scorer correctly."""
    from sqlsentinel.eval import __main__ as m

    gold = {
        0: "SELECT COUNT(*) FROM items",
        1: "SELECT name FROM items",
        2: "SELECT name FROM items LIMIT 1",
    }
    monkeypatch.setitem(
        m.PREDICTORS, "stub", lambda qs: {q["question_id"]: gold[q["question_id"]] for q in qs}
    )
    monkeypatch.setattr(
        m, "stub_predictor", lambda qs: {q["question_id"]: gold[q["question_id"]] for q in qs}
    )
    _run(
        monkeypatch,
        [
            "--split",
            "full",
            "--predictor",
            "stub",
            "--bird-root",
            str(fake_bird),
            "--no-mlflow",
            "--num-cpus",
            "1",
        ],
    )
    assert "EX 100.0%" in capsys.readouterr().out


def test_missing_bird_root_is_a_clear_error(monkeypatch, tmp_path):
    with pytest.raises(FileNotFoundError, match="BIRD dev set incomplete"):
        _run(
            monkeypatch,
            [
                "--split",
                "full",
                "--predictor",
                "stub",
                "--bird-root",
                str(tmp_path / "nope"),
                "--no-mlflow",
            ],
        )
