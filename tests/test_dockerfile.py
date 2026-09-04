"""The Dockerfile must copy in everything the package build reads.

`docker compose up` broke silently once already: `LICENSE` was added and
`pyproject.toml` began declaring `license = { file = "LICENSE" }`, but the
Dockerfile still copied only pyproject/uv.lock/README, so hatchling failed the
editable install with "License file does not exist". Nothing caught it, because
CI never builds the image -- the README simply claimed a command that no longer
worked.

This asserts the invariant directly against pyproject rather than pinning a
filename, so declaring another build-time file fails here instead of in a
build log.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _pyproject() -> dict:
    return tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))


def _dockerfile_copied_files() -> set[str]:
    """Bare files (not directories) named in COPY lines."""
    copied: set[str] = set()
    for line in (ROOT / "Dockerfile").read_text(encoding="utf-8").splitlines():
        if not line.startswith("COPY "):
            continue
        # last token is the destination
        for token in re.split(r"\s+", line[len("COPY ") :])[:-1]:
            copied.add(token.rstrip("/").split("/")[-1] if "/" not in token else token)
    return copied


def test_dockerfile_copies_every_file_pyproject_declares() -> None:
    cfg = _pyproject()["project"]
    required = {cfg["readme"]}
    licence = cfg.get("license")
    if isinstance(licence, dict) and "file" in licence:
        required.add(licence["file"])

    copied = _dockerfile_copied_files()
    missing = {f for f in required if f not in copied}
    assert not missing, (
        f"pyproject declares {sorted(missing)} but the Dockerfile never COPYs "
        "them; the image build will fail at `uv pip install -e .`"
    )


def test_declared_build_files_exist() -> None:
    cfg = _pyproject()["project"]
    assert (ROOT / cfg["readme"]).exists()
    licence = cfg.get("license")
    if isinstance(licence, dict) and "file" in licence:
        assert (ROOT / licence["file"]).exists()
