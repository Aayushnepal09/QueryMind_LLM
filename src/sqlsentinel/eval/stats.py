"""Paired significance testing for technique comparisons.

Two configurations are always evaluated on the *same* questions, so comparing
their independent confidence intervals throws away the pairing and badly
understates power. On dev_50 the independent CI is +/-13 points, which would
declare almost every real effect "not significant"; the paired test can resolve
a 10-question difference on the same 50 items.

McNemar's test looks only at the questions where the two configurations
disagree. Questions both get right, or both get wrong, carry no information
about which is better.
"""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass
class PairedComparison:
    n: int
    a_only: int  # A correct, B wrong
    b_only: int  # B correct, A wrong
    both: int
    neither: int
    p_value: float
    label_a: str = "A"
    label_b: str = "B"

    @property
    def discordant(self) -> int:
        return self.a_only + self.b_only

    @property
    def delta_points(self) -> float:
        """B minus A, in percentage points."""
        return 100.0 * (self.b_only - self.a_only) / self.n if self.n else 0.0

    @property
    def significant(self) -> bool:
        return self.p_value < 0.05

    def verdict(self) -> str:
        if self.discordant == 0:
            return "identical predictions on every question"
        direction = "better" if self.delta_points > 0 else "worse"
        if self.significant:
            return (
                f"{self.label_b} is {abs(self.delta_points):.1f} points {direction} "
                f"than {self.label_a} (p={self.p_value:.3f}, significant)"
            )
        return (
            f"{self.label_b} is {abs(self.delta_points):.1f} points {direction} "
            f"than {self.label_a}, but p={self.p_value:.3f} — not distinguishable "
            f"from chance on {self.n} questions"
        )


def mcnemar(
    correct_a: dict[int, int],
    correct_b: dict[int, int],
    label_a: str = "A",
    label_b: str = "B",
) -> PairedComparison:
    """Exact two-sided McNemar test over questions present in both runs.

    Uses the exact binomial rather than the chi-square approximation: the
    number of discordant pairs is often small (single digits on dev_50), where
    the approximation is unreliable.
    """
    ids = sorted(set(correct_a) & set(correct_b))
    if not ids:
        raise ValueError("no overlapping questions between the two runs")

    a_only = sum(1 for i in ids if correct_a[i] and not correct_b[i])
    b_only = sum(1 for i in ids if correct_b[i] and not correct_a[i])
    both = sum(1 for i in ids if correct_a[i] and correct_b[i])
    neither = len(ids) - a_only - b_only - both

    n_disc = a_only + b_only
    if n_disc == 0:
        p = 1.0
    else:
        k = min(a_only, b_only)
        tail = sum(math.comb(n_disc, i) for i in range(k + 1))
        p = min(1.0, 2 * tail / 2**n_disc)

    return PairedComparison(
        n=len(ids), a_only=a_only, b_only=b_only, both=both, neither=neither,
        p_value=p, label_a=label_a, label_b=label_b,
    )


def wilson_interval(successes: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Wilson score interval, in percentage points.

    Preferred over the normal approximation because it stays inside [0, 100]
    and behaves at small n and extreme proportions.
    """
    if n == 0:
        return (0.0, 0.0)
    p = successes / n
    denom = 1 + z**2 / n
    centre = (p + z**2 / (2 * n)) / denom
    margin = z * math.sqrt(p * (1 - p) / n + z**2 / (4 * n**2)) / denom
    return (100 * max(0.0, centre - margin), 100 * min(1.0, centre + margin))
