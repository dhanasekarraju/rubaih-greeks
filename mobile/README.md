# Rubaih Greeks mobile

Android app for the Delta options bot on the same VPS as futures Rubaih.

- API: `http://103.194.228.130:8088` (cleartext HTTP enabled)
- Token: `RUBAIH_GREEKS_API_TOKEN` (local `config.js` is gitignored)

## GitHub Actions APK (recommended)

Push to `main` (or run **Actions → Build Rubaih Greeks APK → Run workflow**).

1. Optional: add repo secret `RUBAIH_GREEKS_API_TOKEN` so the APK ships with the token baked in.
2. Open the workflow run → download artifact **`rubaih-greeks-apk`**.
3. Install the APK (allow unknown sources). App talks to `:8088` over HTTP; engine should stay dry-run.

## Local setup

```bash
cd mobile
cp config.example.js config.js
# paste host + token into config.js
npm install
```

## Local APK (needs Android SDK)

```bash
npx expo prebuild --platform android
cd android && ./gradlew assembleRelease
# APK: android/app/build/outputs/apk/release/app-release.apk
```
