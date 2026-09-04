"""Visual styling for the review UI.

Kept in one place rather than scattered through the app so the interface can be
restyled without touching review logic.

The design brief is narrow: this is a working queue, not a marketing page. It
should look considered and current, and it should make the *state* of an item
readable before any text is parsed -- a reviewer scanning 99 rows needs to see
severity, not decoration. So colour carries meaning (red/amber/green map to
confidence bands) and nothing else is tinted.
"""

from __future__ import annotations

import streamlit as st

# Semantic palette. Chosen for contrast on both Streamlit themes rather than
# for brand: each pairs a saturated accent with a low-alpha background so text
# stays legible either way.
DANGER = "#ef4444"
WARNING = "#f59e0b"
SUCCESS = "#10b981"
MUTED = "#94a3b8"

CSS = """
<style>
  /* ---------- layout ---------- */
  .block-container { max-width: 1180px; padding-top: 2.2rem; padding-bottom: 4rem; }

  /* ---------- header ---------- */
  .sentinel-head {
    display: flex; align-items: center; gap: 14px; margin-bottom: 4px;
  }
  .sentinel-mark {
    width: 40px; height: 40px; border-radius: 11px; flex: 0 0 40px;
    display: flex; align-items: center; justify-content: center; font-size: 20px;
    background: linear-gradient(135deg, rgba(16,185,129,.22), rgba(59,130,246,.22));
    border: 1px solid rgba(148,163,184,.28);
  }
  .sentinel-title { font-size: 1.55rem; font-weight: 680; letter-spacing: -.02em; line-height: 1.15; }
  .sentinel-sub { color: #94a3b8; font-size: .9rem; margin-top: 1px; }

  /* ---------- stat strip ---------- */
  .stat-row { display: flex; gap: 10px; margin: 20px 0 8px; flex-wrap: wrap; }
  .stat {
    flex: 1 1 130px; padding: 13px 15px; border-radius: 12px;
    background: rgba(148,163,184,.07); border: 1px solid rgba(148,163,184,.16);
  }
  .stat-val { font-size: 1.5rem; font-weight: 660; letter-spacing: -.02em; line-height: 1.1; }
  .stat-key {
    font-size: .7rem; text-transform: uppercase; letter-spacing: .09em;
    color: #94a3b8; margin-top: 3px;
  }

  /* ---------- progress ---------- */
  .track { height: 5px; border-radius: 3px; background: rgba(148,163,184,.18); overflow: hidden; }
  .fill { height: 100%; border-radius: 3px; background: linear-gradient(90deg,#10b981,#3b82f6); }

  /* ---------- queue rows ---------- */
  div[data-testid="stExpander"] {
    border: 1px solid rgba(148,163,184,.18) !important;
    border-radius: 12px !important;
    margin-bottom: 8px !important;
    overflow: hidden;
  }
  div[data-testid="stExpander"] summary { padding: 12px 15px !important; font-weight: 500; }
  div[data-testid="stExpander"] summary:hover { background: rgba(148,163,184,.07); }

  /* ---------- severity pill ---------- */
  .pill {
    display: inline-block; padding: 2px 9px; border-radius: 999px;
    font-size: .68rem; font-weight: 640; letter-spacing: .05em;
    text-transform: uppercase; vertical-align: middle;
  }
  .pill-danger  { background: rgba(239,68,68,.15);  color: #f87171; border: 1px solid rgba(239,68,68,.3); }
  .pill-warning { background: rgba(245,158,11,.15); color: #fbbf24; border: 1px solid rgba(245,158,11,.3); }
  .pill-success { background: rgba(16,185,129,.15); color: #34d399; border: 1px solid rgba(16,185,129,.3); }

  /* ---------- question ---------- */
  .question {
    font-size: 1.16rem; font-weight: 600; line-height: 1.45;
    letter-spacing: -.01em; margin: 4px 0 10px;
  }
  .context {
    font-size: .85rem; color: #94a3b8; border-left: 2px solid rgba(148,163,184,.3);
    padding-left: 11px; margin-bottom: 14px;
  }

  /* ---------- section labels ---------- */
  .label {
    font-size: .7rem; text-transform: uppercase; letter-spacing: .09em;
    color: #94a3b8; font-weight: 640; margin: 18px 0 7px;
  }

  /* ---------- plain-english card ---------- */
  .plain {
    background: rgba(148,163,184,.07); border: 1px solid rgba(148,163,184,.16);
    border-radius: 11px; padding: 14px 16px; margin: 2px 0 6px;
  }
  .plain-lead { font-weight: 600; margin-bottom: 7px; }
  .plain-item { color: #cbd5e1; font-size: .9rem; padding: 2px 0 2px 15px; position: relative; }
  .plain-item:before { content: "—"; position: absolute; left: 0; color: #64748b; }

  /* ---------- buttons ---------- */
  .stButton button { border-radius: 9px !important; font-weight: 550 !important; }

  /* ---------- misc ---------- */
  div[data-testid="stDataFrame"] { border-radius: 10px; overflow: hidden; }
  #MainMenu, footer, header { visibility: hidden; }
  hr { margin: 1.1rem 0; border-color: rgba(148,163,184,.14); }
</style>
"""


def inject() -> None:
    st.markdown(CSS, unsafe_allow_html=True)


def header(title: str, subtitle: str, mark: str = "🛡️") -> None:
    st.markdown(
        f'<div class="sentinel-head"><div class="sentinel-mark">{mark}</div>'
        f'<div><div class="sentinel-title">{title}</div>'
        f'<div class="sentinel-sub">{subtitle}</div></div></div>',
        unsafe_allow_html=True,
    )


def stats(items: list[tuple[str, str]], progress: float | None = None) -> None:
    cells = "".join(
        f'<div class="stat"><div class="stat-val">{v}</div><div class="stat-key">{k}</div></div>'
        for k, v in items
    )
    st.markdown(f'<div class="stat-row">{cells}</div>', unsafe_allow_html=True)
    if progress is not None:
        st.markdown(
            f'<div class="track"><div class="fill" style="width:{progress:.1f}%"></div></div>',
            unsafe_allow_html=True,
        )


def severity(confidence: float) -> tuple[str, str, str]:
    """Map a confidence score to (pill-class, label, dot).

    Bands match the routing thresholds, so the colour a reviewer sees is the
    same signal the router acted on rather than a separate scale. The dot is
    for collapsed rows: Streamlit renders expander labels as plain text, so a
    coloured glyph is the only way to carry severity there.
    """
    if confidence < 0.34:
        return "pill-danger", "high risk", "🔴"
    if confidence < 0.7:
        return "pill-warning", "uncertain", "🟡"
    return "pill-success", "likely ok", "🟢"


def pill(confidence: float) -> str:
    cls, label, _ = severity(confidence)
    return f'<span class="pill {cls}">{label}</span>'
