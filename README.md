# Flip Messenger

This repository contains a minimal Android 11 keypad messaging client and a Go
server. The server uses
[`mautrix/gmessages`](https://github.com/mautrix/gmessages) to link to Google
Messages for Web and expose SMS and RCS conversations to the phone.

Implemented now:

- Google-account cookie and emoji pairing with server-only session storage
- SMS and RCS conversation discovery, history import, and live incoming text
- durable outgoing queue with idempotent commands and remote-echo deduplication
- dynamic channel refresh while the Android client is running
- HTTPS with bearer-token authentication
- cursor-based bootstrap and incremental synchronization
- Android channel list, conversation view, text entry, and background long-polling
- local incoming-message notifications without Google Play Services
- Android key/scan-code logging for continued TCL button discovery

Attachments are currently shown as text placeholders. Reactions, receipts,
typing, WebSockets, and WebRTC are not implemented yet. FCM is intentionally not
used because the target TCL firmware does not include Google Play Services.

## 1. Configure the server

Requirements: Go 1.27 (the module and its pinned `mautrix/gmessages` revision
require the modern Go toolchain). HTTPS is the default and requires a
certificate valid for the hostname or IP used by the phone.

```sh
cd server
cp config.example.json config.json
```

Edit `config.json`:

- replace `api_token` with a long random value (`openssl rand -hex 32`)
- leave `google_messages.enabled` set to `true`
- choose how many recent messages to import per conversation with
  `google_messages.history` (maximum 500)
- set `google_messages.whitelist` to the participant phone numbers or Google
  conversation IDs that may appear in the client; a group is allowed when any
  non-self participant is listed, an empty list allows none, and omitting the
  field disables filtering
- set `tls_cert` and `tls_key` to the certificate files
- set `debug` to `true` for HTTP, synchronization, outbox, and Google Messages
  protocol diagnostics; message bodies, cookies, and tokens are not logged

For normal use, prefer a DNS name and publicly trusted certificate. For LAN
development, `mkcert` can create a certificate for the server hostname/IP. Its
root CA must also be installed as a user CA on the TCL. Debug builds trust user
CAs; release builds trust only system CAs.

For temporary trusted-LAN development without TLS, set `allow_http` to `true`,
change `listen` to `:8080`, and use an `http://` client URL. Both debug and
release APKs permit cleartext HTTP, so use it only on a trusted network: the
bearer token and all message data are sent without transport encryption.

Start the server from the `server` directory:

```sh
go run ./cmd/flip-messenger-server -config config.json
```

When Google Messages is enabled, the server does not create loopback echoes.

## 2. Pair Google Messages

Google has disabled the old QR pairing flow. Manual authentication uses cookies
from a dedicated private browser session and an emoji confirmation on the
primary phone:

1. Open this URL in a **private Firefox window** and log into the same Google
   account used by Google Messages:

   <https://accounts.google.com/AccountChooser?continue=https://messages.google.com/web/config>

   Do not navigate elsewhere in that window. Browsers with Device Bound Session
   Credentials enabled may not work; Firefox currently does not implement it.

2. Open developer tools and its Network tab, reload the page, select the
   `/web/config` request, and choose **Copy as cURL (bash)**.

3. Save the copied command to a private temporary file without putting the
   cookies into shell history:

```sh
umask 077
cat > /tmp/gmessages-auth.curl
# Paste the copied cURL command, then press Ctrl-D.
```

4. From the repository root, submit it to the server. The response contains the
   emoji to select in Google Messages. The URL below matches the current local
   HTTP development setup; use your HTTPS server URL otherwise:

```sh
token=$(python -c 'import json; print(json.load(open("server/config.json"))["api_token"])')
server_url=http://127.0.0.1:8080
curl -sS -X POST \
  -H "Authorization: Bearer $token" \
  -H 'Content-Type: text/plain' \
  --data-binary @/tmp/gmessages-auth.curl \
  "$server_url/v1/google/pair" | python -m json.tool
rm -f /tmp/gmessages-auth.curl
```

5. Open Google Messages on the primary phone and tap the matching emoji when
   prompted. Pairing completes asynchronously. Check status until `paired` is
   `true`:

```sh
curl -sS -H "Authorization: Bearer $token" \
  "$server_url/v1/google/status" | python -m json.tool
```

Instead of copied cURL, the pairing endpoint also accepts a JSON object mapping
cookie names to values. `SID`, `HSID`, `OSID`, `SSID`, `APISID`, and `SAPISID`
are required; Google may also require `__Secure-1PSIDTS`.

After pairing, the session credentials are stored with mode `0600` at
`google_messages.session_file`. Keep this file private and backed up. The server
reconnects and imports configured history on restart. If Google revokes or
expires the link, clear the local pairing and then pair again:

```sh
curl -X DELETE -H 'Authorization: Bearer YOUR_TOKEN' \
  https://YOUR_SERVER:8443/v1/google/session
```

Check it from another terminal:

```sh
curl https://YOUR_SERVER:8443/healthz
curl -H 'Authorization: Bearer YOUR_TOKEN' \
  https://YOUR_SERVER:8443/v1/bootstrap
```

State is written atomically to the configured `data_file`. Keep both that file
and the attachment-free configuration backed up; deleting the state file
resets all messages and cursors.

## 3. Build and install the Android client

The server URL and matching token are compiled into this prototype APK:

```sh
export ANDROID_HOME="${ANDROID_HOME:-$HOME/Android/Sdk}"
./gradlew \
  -PserverUrl=https://YOUR_SERVER:8443 \
  -PapiToken=YOUR_TOKEN \
  assembleDebug
adb install -r app/build/outputs/apk/debug/app-debug.apk
adb shell am start -n com.noskcaj19.flipmessenger/.MainActivity
```

If these Gradle properties are omitted, the app intentionally points at the
invalid hostname `flip-server.invalid`.

The app runs a foreground service with a permanent “Listening for messages”
notification. It keeps an authenticated long poll open to the server and posts
a normal local notification for each incoming message while the activity is in
the background. To prevent Doze from suspending that connection on the TCL, add
the app to the device-idle allowlist once after installation:

```sh
adb shell dumpsys deviceidle whitelist +com.noskcaj19.flipmessenger
```

The service restarts after process termination and device reboot. Android does
not restart it after the user explicitly force-stops the app; launch Flip
Messenger once to start it again.

## Phone controls

- D-pad up/down: select a channel
- D-pad center or right soft key: open the selected channel
- Phone keypad/IME: compose text
- D-pad center while composing: send
- Left soft key or Back: return to the channel list
- Call: displays the current “voice not implemented” status

The Android IME currently determines how multi-tap/T9 entry behaves. A custom
T9 composer is deferred. All hardware key events remain visible with:

```sh
adb logcat -s FlipMessenger:I '*:S'
```

For buttons reserved by TCL firmware rather than delivered to the app:

```sh
adb shell getevent -lt
```

## Text protocol

`GET /v1/bootstrap` returns the configured channels, complete current message
snapshot, and synchronization cursor. `GET /v1/sync?after=<cursor>` returns up
to 200 committed `message.created` events after that cursor, plus the current
dynamic channel list. Supplying `wait=25s` holds an otherwise empty sync request
until an event is committed or the wait expires; waits longer than 25 seconds
are rejected.

Text is sent with `POST /v1/messages`:

```json
{
  "v": 1,
  "kind": "command",
  "type": "message.send",
  "command_id": "cmd_unique",
  "body": {
    "client_message_id": "cmsg_unique",
    "channel_id": "jack-discord",
    "text": "Hello"
  }
}
```

Retrying the same command ID and body returns the original acknowledgement.
Reusing a command ID with a different body returns `idempotency_conflict`.

For Google Messages channels, acceptance means the command and outgoing message
are durably queued by this server. The queue is retried until Google Messages
accepts it; carrier/RCS delivery can occur later. A remote echo with the same
client message ID resolves the queue entry without creating a duplicate.

## Google Messages dependency caveats

`mautrix/gmessages` is an unofficial reverse-engineered Messages for Web client.
Google may change the private protocol, and sending depends on the paired
Android phone remaining online and responsive. The dependency is pinned to an
exact commit through `server/go.mod` for reproducible behavior.

`mautrix/gmessages` is licensed AGPL-3.0-or-later. Network deployment or
distribution may require providing the corresponding source under the AGPL;
review those obligations before deploying this server to other users.
