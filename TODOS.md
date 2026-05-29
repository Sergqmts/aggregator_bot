# TODOS

## Near-duplicate detection
**Status:** Deferred (consider after Approach A is stable)
**What:** Add URL-based near-duplicate detection so the same story from two channels appears only once.
**Why:** MD5 on text catches exact copies only. Two channels reposting the same news = duplicates in the feed = eroded trust before launch.
**Context:** Design premis 6 explicitly defers this. Simple implementation: store `entry.link` URL as a secondary dedup key in `sent_hashes`. If the same URL appears from two sources, skip the second.
**Approach:** Add `source_url TEXT` column to `sent_hashes`. Check `SELECT 1 FROM sent_hashes WHERE source_url = ?` before MD5 check.
**Depends on:** Approach A running stably first.

## Topic routing — multiple channel publish
**Status:** Post-launch
**What:** Route posts to separate Telegram channels by topic (cooking, IT, auto, etc.) instead of single aggregator.
**Why:** Platform may have category-based navigation. A single mixed feed is a stopgap, not the final UX.
**Context:** Low engineering cost — one TARGET_CHANNEL_ID per topic in .env, routing dict keyed by topic in publish loop.
**Depends on:** Launch decision on channel structure + channels existing.

## Self-hosted RSSHub (Docker fallback)
**Status:** Contingency (trigger: rsshub.app unstable for 1 week)
**What:** `docker run -d rsshub/rsshub` on the same VPS.
**Why:** rsshub.app public instance has no SLA. Self-hosted means you control availability.
**Runbook:** `docker run -d -p 1200:1200 --name rsshub rsshub/rsshub` → update RSSHUB_BASE_URL in .env.
**Depends on:** VPS having Docker installed + rsshub.app showing actual instability.
