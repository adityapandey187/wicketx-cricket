# WicketX — cricket scores prototype (web + Android)

Cricbuzz-style cricket companion with original vintage design, branding and content.
Live web app: https://wicketx.appwrite.network/

## Status
- Web app: React + TypeScript + Vite (single-file build), hosted on Appwrite Sites (Free plan).
- Android: Capacitor wrapper, app id in.adityapandey.wicketx, debug APK compiles (runtime unverified: no emulator available).
- Backend: Appwrite Cloud (Free plan, region FRA), project "WicketX".
- All scores, fixtures, stories and rankings are illustrative demo data until a licensed provider is chosen.

## Backend (Appwrite)
- Database `wicketx`
  - Table `matches`: demo match documents (`payload` JSON string, `dataMode` string). Permission: read for anyone; no client writes.
  - Table `saved_matches`: per-user saved matches (`matchId` string). Row security ON; create for signed-in users only; each row carries user-scoped read/delete permissions, so users can only see and remove their own saves.
- Auth: email + password, optional. Everything is browsable as a guest; an account is only needed to sync saved matches across devices.
- Client config is public (endpoint, project id, database id) and lives in `.env` — see `.env.example`.
- Verified by a live two-account permission test (`scripts/appwrite-test.cjs`): anonymous reads allowed, anonymous writes rejected, cross-user read/delete rejected.

## Local development
1. `npm install`
2. Copy `.env.example` to `.env` (values are public client config).
3. `npm run dev`, then open http://localhost:5173

## Tests and build
- `npx playwright test` — 6 tests covering layout, routing and the optional-account flow.
- `npm run build` — production build into `dist/`.
- Android: `npx cap sync android`, then `cd android && JAVA_HOME=/home/sandbox/jdk ./gradlew assembleDebug` (needs JDK 17+; system default is Java 11).

## Deploy (Appwrite Sites)
Manual static deploy of `dist/`: create site, upload `tar.gz` of the dist contents, framework React with install/build commands cleared and output directory `./`. Add the site hostname as a Web platform in the project so the API accepts its requests.

## Repository artifacts
- `1-5-WicketX-source-appwrite.zip` — full source with Appwrite integration (supersedes `1-WicketX-source.zip`).
- `2-6-WicketX-preview.html` — built web app preview from `dist/index.html` (supersedes `2-WicketX-preview.html`).
- `3-7-WicketX-demo-debug.apk` — Android debug build with backend config (supersedes `3-WicketX-demo-debug.apk`).
- `3-README.md` — this file.

Older `Boundary-*` files are superseded checkpoints from before the rename.

## Legal
Independent prototype. Not affiliated with Cricbuzz or any cricket board. No Cricbuzz assets, copy or data.
