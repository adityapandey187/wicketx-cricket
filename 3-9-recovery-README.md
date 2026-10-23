# Recovery artifacts (batch 9, 23 Oct 2026)

IMPORTANT: the batch-8 source zip `1-8-WicketX-source-live.zip` is corrupt (2 bytes; the upload failed silently at commit time). The batch-8 editable source tree is not currently recoverable.

These recovery backups are BUILD ARTIFACTS, not the editable source:

- `1-9-recovery-wicketx-dist-batch8.tar.gz` (1.07 MB) - the exact production build downloaded from Appwrite Sites (deployment 6ada6333328ac59e8a51): the deployed single-file index.html (all JS/CSS inlined) plus icons and manifest. This is what runs at https://wicketx.appwrite.network/ today.
- `2-9-recovery-cricketdata-sync-main.js` - full editable source of the Appwrite Function that syncs CricketData scores into the matches table hourly (downloaded from the Appwrite function deployment). This one IS true source.

Intact older editable source: `1-5-WicketX-source-appwrite.zip` (4.17 MB, pre-live-data version).

To recover the dist: `tar -xzf 1-9-recovery-wicketx-dist-batch8.tar.gz`.
