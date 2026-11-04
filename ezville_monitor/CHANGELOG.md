# Changelog

## 0.7.4

- Add opt-in NUC-local MOES read measurements at KST 00/06/12/18, with durable per-attempt results and signed read-only reports.
- Add an explicit one-time commissioning ID; restarts never repeat the same ID or scheduled slot.
- Preserve existing telemetry and credentials; never send IR, pairing, leave or rejoin commands.

## 0.7.3

- Aggregate regularly recurring fingerprint-only candidate windows into bounded, low-priority groups.
- Group only occurrences that share the recurring cadence and phase; preserve off-phase exceptions and localized/count-change candidates in the primary list.
- Report grouped occurrence totals and examples without deleting original profile evidence.
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
