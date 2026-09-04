"""Record an animated GIF of the review UI.

    uv run streamlit run app/review_ui.py --server.headless true --server.port 8610
    uv run python scripts/record_demo.py

Shows the app being *used*, not just displayed: asking a live question and
watching a typo get flagged, then filtering the review queue, opening a query,
typing a note, recording a decision and watching the counters move, and finally
switching to the engineer view.

State changes are the point. A reviewer watching this should see the queue
respond — the pending count drop, an item leave the list — because that is what
distinguishes a working tool from a screenshot.

The decisions database is reset before recording so the demo always starts from
an empty queue and the counters move visibly.

Written as a script rather than captured by hand so the demo can be re-recorded
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
GLIDE = 150  # intermediate scroll and typing frames
READ = 900  # frames carrying information
BEAT = 450  # transitions


def _reset_decisions() -> None:
    """Clear recorded review decisions so the demo starts from a full queue.

    Without this the counters do not move on a re-record, because the item the
    walkthrough decides on was already decided last time.
    """
    db = REPO_ROOT / "results" / "review_decisions.db"
    if not db.exists():
        return
    # Delete the rows rather than the file: the running Streamlit process holds
    # an open handle, and on Windows that makes unlink fail outright.
    import sqlite3

    with sqlite3.connect(db) as conn:
        conn.execute("DELETE FROM decisions")
        conn.commit()


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

        # --- ask a live question, with a typo in it.
        # The typo is the point: the model answers it confidently either way,
        # and the checker is what makes the guess visible.
        ask = page.get_by_role("textbox", name="Ask a question").first
        ask.click()
        for chunk in ("What is the ", "alignmnt of ", "the Hulk?"):
            ask.press_sequentially(chunk, delay=26)
            shot(page, GLIDE + 140)
        shot(page, BEAT)

        page.get_by_role("button", name="Ask").first.click()
        page.wait_for_timeout(1500)
        shot(page, BEAT)
        # generation runs on a local model; wait for the verdict to appear
        page.get_by_text("Possible typo", exact=False).first.wait_for(timeout=180_000)
        page.wait_for_timeout(1200)
        shot(page, READ + 900)  # "alignmnt -> alignment"
        glide(320)
        shot(page, READ + 500)  # the answer and the plain-English description

        # --- ask again, this time with a word that is not a column at all.
        # "strongest" names a *value* in the data rather than a table or column,
        # so the checker explains the reading it is using instead of flagging it.
        glide(-320)
        ask.fill("")
        for chunk in ("who is ", "the strongest?"):
            ask.press_sequentially(chunk, delay=26)
            shot(page, GLIDE + 140)
        shot(page, BEAT)

        page.get_by_role("button", name="Ask").first.click()
        page.get_by_text("Reading", exact=False).first.wait_for(timeout=180_000)
        page.wait_for_timeout(1200)
        shot(page, READ + 900)  # "Reading strongest as Strength in attribute_name"
        glide(300)
        shot(page, READ + 500)

        # --- over to the review queue
        page.get_by_role("tab", name="Review queue", exact=False).first.click()
        page.wait_for_timeout(2200)
        shot(page, READ)

        # --- filter the queue with the confidence slider.
        # Driven by keyboard rather than by dragging: Streamlit renders the
        # slider as input[type=range], and arrow keys move it deterministically
        # while a drag depends on pixel geometry that shifts with the layout.
        slider = page.get_by_label("Show confidence at or below")
        slider.click()
        page.wait_for_timeout(400)
        for _ in range(6):  # 1.00 -> 0.70, the routing threshold
            slider.press("ArrowLeft")
            page.wait_for_timeout(220)
            shot(page, GLIDE)
        page.wait_for_timeout(1800)
        shot(page, READ + 500)  # the list narrows to the riskiest items

        for _ in range(6):  # back to the full queue
            slider.press("ArrowRight")
            page.wait_for_timeout(120)
        page.wait_for_timeout(1600)

        # --- open a flagged query.
        # Streamlit renders every row's controls into the DOM whether the
        # expander is open or not, so selectors must be scoped to the row --
        # a bare .first picks a different item's button.
        QUESTION = "How strong is the Hulk?"
        row = page.locator('div[data-testid="stExpander"]').filter(has_text=QUESTION).first
        row.scroll_into_view_if_needed()
        page.wait_for_timeout(400)
        shot(page, BEAT)
        row.get_by_text(QUESTION, exact=False).first.click()
        page.wait_for_timeout(2400)
        shot(page, BEAT)

        # --- confidence in words
        glide(320)
        shot(page, READ)

        # --- the answer it would return
        glide(300)
        shot(page, READ + 400)

        # --- what the query does
        glide(300)
        shot(page, READ)

        # --- type a reviewer note, in chunks so it reads as typing
        glide(300)
        note = row.get_by_role("textbox").first
        note.scroll_into_view_if_needed()
        note.click()
        for chunk in ("Strength 100 looks ", "right, but this ", "misses durability"):
            note.press_sequentially(chunk, delay=22)
            shot(page, GLIDE + 140)
        shot(page, READ)

        # --- record the decision, then scroll up to watch the counters move.
        # Target the button, not the <p> inside it: the text node has no box of
        # its own, so Playwright reports it as not visible and the click hangs.
        reject = row.get_by_role("button", name="This looks wrong").first
        reject.scroll_into_view_if_needed()
        page.wait_for_timeout(500)
        shot(page, BEAT)
        reject.click()
        page.wait_for_timeout(2800)
        page.mouse.wheel(0, -2000)
        page.wait_for_timeout(1200)
        shot(page, READ + 800)  # reviewed 0 -> 1, pending 99 -> 98

        # --- engineer mode: same queue, SQL first, editable
        page.get_by_role("radio", name="Engineer (SQL)").first.click()
        page.wait_for_timeout(2800)
        shot(page, BEAT)

        NEXT = "What percentage of Japanese"
        nxt = page.locator('div[data-testid="stExpander"]').filter(has_text=NEXT).first
        nxt.scroll_into_view_if_needed()
        page.wait_for_timeout(500)
        nxt.get_by_text(NEXT, exact=False).first.click()
        page.wait_for_timeout(2600)
        shot(page, BEAT)
        glide(340)
        shot(page, READ + 700)

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
        _reset_decisions()
        record(args.url, Path(args.out))
    except Exception as e:
        # Page text can contain emoji; a cp1252 console cannot encode them and
        # the traceback print would itself raise, hiding the real failure.
        detail = str(e).encode("ascii", "replace").decode("ascii")
        print(f"recording failed: {type(e).__name__}: {detail[:400]}")
        print(f"is the review UI running at {args.url}?")
        sys.exit(1)


if __name__ == "__main__":
    main()
