# Changelog

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
