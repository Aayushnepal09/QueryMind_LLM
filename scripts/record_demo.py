"""Record an animated GIF of the review UI.

    uv run streamlit run app/review_ui.py --server.headless true --server.port 8610
    uv run python scripts/record_demo.py

Walks the plain-English review flow a non-engineer would follow — queue, then
one flagged query, its confidence in words, the answer it would return, what
the query does, and the decision — then switches to the engineer view to show
the same item as SQL.

Written as a script rather than done by hand so the demo can be re-recorded
after a UI change instead of going stale.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RESULTS = REPO_ROOT / "results"

# (caption, milliseconds to hold) — longer on the frames that carry meaning.
#
# Held via per-frame duration rather than by repeating identical frames:
# Pillow's optimize pass collapses consecutive duplicates, so repetition
# silently produces a 6-frame GIF that flashes past.
STEPS = [
    ("the queue", 3000),
    ("open a flagged query", 2200),
    ("confidence in words, not a number", 3200),
    ("the answer it would return", 3800),
    ("what the query does, in plain English", 3400),
    ("approve, reject, or unsure", 2800),
    ("engineers get the same queue with SQL", 3800),
]


def record(url: str, out: Path, width: int = 1180, height: int = 840) -> None:
    from playwright.sync_api import sync_playwright

    frames: list[bytes] = []
    holds: list[int] = []

    def shot(page, hold_ms: int) -> None:
        frames.append(page.screenshot())
        holds.append(hold_ms)

    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        page = browser.new_page(viewport={"width": width, "height": height})
        page.goto(url, wait_until="networkidle")
        page.wait_for_timeout(3500)

        # 1: the queue
        shot(page, STEPS[0][1])

        # 2: open an item with a result worth looking at
        target = page.get_by_text("How strong is the Hulk?", exact=False).first
        target.scroll_into_view_if_needed()
        page.wait_for_timeout(400)
        shot(page, STEPS[1][1])
        target.click()
        page.wait_for_timeout(2500)

        # 3: question + confidence in words
        page.mouse.wheel(0, 320)
        page.wait_for_timeout(900)
        shot(page, STEPS[2][1])

        # 4: the answer table
        page.mouse.wheel(0, 300)
        page.wait_for_timeout(900)
        shot(page, STEPS[3][1])

        # 5: plain-English description
        page.mouse.wheel(0, 300)
        page.wait_for_timeout(900)
        shot(page, STEPS[4][1])

        # 6: the decision controls
        page.mouse.wheel(0, 320)
        page.wait_for_timeout(900)
        shot(page, STEPS[5][1])

        # 7: engineer mode. Scroll back to the item first, then down just far
        # enough to frame the SQL editor rather than past it to the buttons.
        page.get_by_text("Engineer (SQL)", exact=False).first.click()
        page.wait_for_timeout(3000)
        page.get_by_text("How strong is the Hulk?", exact=False).first.scroll_into_view_if_needed()
        page.wait_for_timeout(600)
        page.mouse.wheel(0, 260)
        page.wait_for_timeout(1000)
        shot(page, STEPS[6][1])

        browser.close()

    _assemble(frames, holds, out)


def _assemble(frames: list[bytes], holds: list[int], out: Path) -> None:
    import io

    from PIL import Image

    images = [Image.open(io.BytesIO(f)).convert("P", palette=Image.ADAPTIVE) for f in frames]
    out.parent.mkdir(parents=True, exist_ok=True)
    images[0].save(
        out,
        save_all=True,
        append_images=images[1:],
        duration=holds,
        loop=0,
        optimize=True,
    )
    size_mb = out.stat().st_size / 1024 / 1024
    print(f"wrote {out}  ({len(images)} frames, {size_mb:.2f} MB)")
    if size_mb > 10:
        print("  warning: over 10 MB — GitHub will not render this inline")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8610")
    ap.add_argument("--out", default=str(RESULTS / "review-ui-demo.gif"))
    args = ap.parse_args()

    try:
        record(args.url, Path(args.out))
    except Exception as e:
        print(f"recording failed: {type(e).__name__}: {e}")
        print(f"is the review UI running at {args.url}?")
        sys.exit(1)


if __name__ == "__main__":
    main()
