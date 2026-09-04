"""Tests for the review UI's presentation helpers.

`app/` was outside the test path until a `severity()` signature change broke
`pill()` at runtime -- caught only by opening the page. These cover the pure
functions so that class of break fails in CI instead.
"""

import pytest
import theme


@pytest.mark.parametrize(
    ("confidence", "expected_label"),
    [
        (0.0, "high risk"),
        (0.33, "high risk"),
        (0.34, "uncertain"),
        (0.67, "uncertain"),
        (0.7, "likely ok"),
        (1.0, "likely ok"),
    ],
)
def test_severity_bands(confidence, expected_label):
    _, label, _ = theme.severity(confidence)
    assert label == expected_label


def test_severity_bands_match_the_router_thresholds():
    """The colour a reviewer sees must be the signal the router acted on."""
    from sqlsentinel.router import Router

    r = Router(threshold=0.7)
    for conf in (0.0, 0.33, 0.5, 0.69):
        assert not r.route("SELECT a FROM t WHERE b=1", conf, row_count=1).auto
        assert theme.severity(conf)[1] != "likely ok"
    assert r.route("SELECT a FROM t WHERE b=1", 0.7, row_count=1).auto
    assert theme.severity(0.7)[1] == "likely ok"


def test_severity_returns_three_parts():
    """Regression: pill() unpacked two values after a third was added."""
    assert len(theme.severity(0.5)) == 3


def test_every_band_has_a_distinct_dot():
    dots = {theme.severity(c)[2] for c in (0.1, 0.5, 0.9)}
    assert len(dots) == 3


def test_pill_renders_class_and_label():
    html = theme.pill(0.1)
    assert "pill-danger" in html and "high risk" in html


@pytest.mark.parametrize("confidence", [0.0, 0.25, 0.5, 0.75, 1.0])
def test_pill_never_raises(confidence):
    assert theme.pill(confidence).startswith("<span")


def test_css_is_wrapped_in_a_style_tag():
    assert theme.CSS.strip().startswith("<style>")
    assert theme.CSS.strip().endswith("</style>")
