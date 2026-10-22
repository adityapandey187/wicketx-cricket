# WicketX - backup

Live: https://wicketx.appwrite.network/

## What's in this batch (8)

- `1-8-WicketX-source-live.zip` - full current source: React/Vite web app (src/), Capacitor Android project, Appwrite integration (src/appwrite.ts, AccountPanel.tsx), Playwright tests, plus `function/cricketdata-sync/` (the Appwrite Function that syncs CricketData into the Appwrite matches table every 60 min, API key server-side only).
- `2-8-WicketX-preview.html` - self-contained preview of the deployed build (open in a browser).
- `3-8-WicketX-live-debug.apk` - Android debug build of the same bundle (debug-signed; install with adb or allow unknown sources).

## State (23 Oct 2026)

- Near-live scores from CricketData (cricketdata.org) via the server-side function; the API key is stored as an Appwrite function variable, never in the client.
- Free plan limits: CricketData 100 hits/day (sync uses ~24/day at hourly), Appwrite free tier (function interval min 60 min). CricketData free tier is personal/non-commercial use only.
- Matches page groups matches into International / Domestic / Leagues; India matches sort first; home page shows India first, then other internationals.
- Cards marked LIVE are near-live; everything else is labelled demo data.
- Demo scorecards inside match detail are sample data ("Sample scorecard").
- 6/6 Playwright tests pass; APK is a debug build, runtime-tested only as far as the web bundle (not on a device).
