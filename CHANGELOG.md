# Changelog

## 1.7.0 (2026-07-07)

* Retry with an `eta` now schedules the job for the requested time instead of enqueuing it immediately.
* Declares its `z4j-bare` dependency (the console script imports it).
* Python 3.11 is now the minimum supported version (3.10 dropped).
* Part of the coordinated 1.7.0 fleet release (unified fleet version, green lint/format/import-boundary gate).

## 1.4.0 (2026-05-02)

Initial 1.4.0 release: RQ engine adapter. Redis-backed; Django and Flask both first-class.
