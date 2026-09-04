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

# Milliseconds each frame is held.
#
# Held via per-frame duration rather than by repeating identical frames:
# Pillow's optimize pass collapses consecutive duplicates, so repetition
# silently produces a handful of frames that flash past.
#
# Two speeds. Scroll steps are short so movement reads as motion rather than as
# jumps; the frames a viewer needs to actually read are held long enough to
# read. An earlier cut used 3-5s uniformly, which was both slow and jerky.
GLIDE = 200  # intermediate scroll frames
READ = 1250  # frames carrying information
BEAT = 650  # transitions


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

        def glide(px: int, steps: int = 3) -> None:
            """Scroll in small increments, capturing each, so it reads as motion."""
            for _ in range(steps):
                page.mouse.wheel(0, px // steps)
                page.wait_for_timeout(260)
                shot(page, GLIDE)

        # the queue
        shot(page, READ)

        # open a flagged query
        target = page.get_by_text("How strong is the Hulk?", exact=False).first
        target.scroll_into_view_if_needed()
        page.wait_for_timeout(400)
        shot(page, BEAT)
        target.click()
        page.wait_for_timeout(2200)
        shot(page, BEAT)

        # confidence in words
        glide(320)
        shot(page, READ)

        # the answer it would return
        glide(300)
        shot(page, READ + 400)

        # what the query does
        glide(300)
        shot(page, READ)

        # the decision controls
        glide(320)
        shot(page, READ)

        # engineer mode: scroll back to the item, then just far enough to frame
        # the SQL editor rather than past it to the buttons
        page.get_by_text("Engineer (SQL)", exact=False).first.click()
        page.wait_for_timeout(2600)
        page.get_by_text("How strong is the Hulk?", exact=False).first.scroll_into_view_if_needed()
        page.wait_for_timeout(600)
        shot(page, BEAT)
        page.mouse.wheel(0, 260)
        page.wait_for_timeout(900)
        shot(page, READ + 600)

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
    print(f"wrote {out}  ({len(images)} frames, {sum(holds) / 1000:.1f}s, {size_mb:.2f} MB)")
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
