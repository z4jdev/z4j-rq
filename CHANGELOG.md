# Changelog

## 1.10.0 (2026-08-28)

* Carried with the coordinated fleet release. No behaviour changed.

## 1.9.1 (2026-08-27)

* The dead-letter requeue is now reachable. This adapter's implementation was
  already complete and is safe for a structural reason: `FailedJobRegistry` is
  RQ's dead-letter concept, and `registry.requeue` consumes the entry and
  preserves its original routing rather than publishing a copy. What was missing
  was a brain endpoint that issued the command, so nothing could trigger it.
* Otherwise carried with the coordinated fleet release.

## 1.9.0 (2026-08-25)

* Purge action correction carried with the fleet release.
* Correct the RQ dependency floor to 1.10.1. RQ 1.10.0 cannot import on Python 3.12 and later.

## 1.8.0 (2026-07-23)

* The adapter now attests its safe by-reference retry contract to the exact agent session and requires the coordinated 1.8.0 bare/core runtime.
* Retry now re-runs the original failed job BY REFERENCE (`FailedJobRegistry.requeue`) rather than reconstructing arguments the brain stores redacted; the explicit operator-override path is retained and still requires a brain-supplied task name.
* Destructive actions offload their broker I/O, with a consecutive-timeout circuit breaker on bulk retry so a hung Redis can no longer freeze the agent's receive loop.
* Part of the coordinated 1.8.0 fleet release (unified fleet version, green lint/format/import-boundary gate).

## 1.7.0 (2026-07-07)

* Retry with an `eta` now schedules the job for the requested time instead of enqueuing it immediately.
* Declares its `z4j-bare` dependency (the console script imports it).
* Python 3.11 is now the minimum supported version (3.10 dropped).
* Part of the coordinated 1.7.0 fleet release (unified fleet version, green lint/format/import-boundary gate).

## 1.4.0 (2026-05-02)

Initial 1.4.0 release: RQ engine adapter. Redis-backed; Django and Flask both first-class.
