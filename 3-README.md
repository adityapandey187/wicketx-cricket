# Boundary Cricket

An original indigo-and-lime cricket website and bundled Android prototype. Not affiliated with Cricbuzz, the ICC or any cricket board. No Cricbuzz code, branding, news, photos or commentary is included.

## Status

This is a working **demo**, not a production live-score service. Every score, fixture, ranking and squad is illustrative. Stories are original evergreen explainers, not live news. No data provider is connected. No paid service has been enabled.

Included: responsive home, match filters and search, match details, one detailed sample scorecard/commentary/squad, schedules, sample rankings, stories, device-local saved matches and light/dark theme. No authentication, payments, notifications, video streaming or cloud sync.

## Run locally

Node 22.12+ is required.

```sh
npm ci
npm run dev
npm test
npm run build
```

Browser tests use `/usr/bin/google-chrome` by default; edit `playwright.config.ts` for another Chrome executable. Tests cover navigation, filtering, search, saved/theme persistence and overflow checks at 320, 390, 768 and 1440 px.

## Android

The `android/` project uses Capacitor. Web assets are bundled in the APK, not loaded from a remote site. Use JDK 21 and Android SDK 36.

```sh
npm run android:sync
cd android
./gradlew assembleDebug --no-daemon --max-workers=1
```

The debug APK is for testing only. It is not a Play Store release. A release needs a user-controlled signing key, actual-device testing, a privacy/data policy review and distribution decisions. Keep keystores and credentials out of git. Existing release signing keys must go through a secure secret store.

## Data and next steps

Before real scores are enabled, choose a provider that explicitly permits the intended display/redistribution and commercial or non-commercial use. Confirm coverage, delay, quota and attribution. Store the key server-side only. Never bundle it in the website or Android app.

CricketData was reviewed as an option, not integrated. Its pricing page states 100 free requests/day; terms limit commercial use. That is not sufficient evidence for a free public commercial real-time product.

- https://cricketdata.org/pricing/
- https://cricketdata.org/terms-of-use/

A production architecture should fetch and cache centrally, enforce a shared daily request cap, mark stale scores, and return explicit provider errors. Do not label cached or demo data as live. No Supabase project is required for the current demo. A future cloud-synced service can use a separate approved database.

## Privacy

Saved matches and theme are stored in localStorage on the device. No analytics, account or personal data collection is included. Fonts are bundled locally with their open font licenses. No external font requests are required.

## Verification record

The web build and all five Playwright test groups passed locally. Npm audit reported zero vulnerabilities at handoff. Android debug build succeeded. Android runtime testing remains blocked: no hardware acceleration and insufficient free RAM for the software emulator in the build environment. Do not describe the APK as runtime-tested or production-ready.
