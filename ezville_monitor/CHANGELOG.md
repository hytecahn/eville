# Changelog

## 0.7.3

- Aggregate regularly recurring fingerprint-only candidate windows into bounded, low-priority groups.
- Preserve localized/count-change and rare variant candidates in the primary list; report grouped occurrence totals and examples.
- Preserve passive collection and existing database format; issue no HA or device commands.

## 0.7.2

- Evaluate candidates using complete local windows and observed same-time baselines instead of unrelated historical gaps.
- Consolidate contiguous newly observed signatures into one sustained episode instead of repeated bucket candidates.
- Keep current and historical incomplete-window counts explicit; never infer missing profiles as zero.
- Preserve passive collection and existing database format; issue no HA or device commands.

## 0.7.1

- Add signed read-only action-window correlation for operator-reported action times.
- Return bounded packet signatures, counts, fingerprints and coverage without raw frames.
- Preserve passive collection and existing database format; issue no HA or device commands.
