# Flip Messenger loopback prototype

This repository contains a minimal Android 11 keypad messaging client and a Go
loopback server. Channels are configured on the server. Sending text to any
channel durably stores the message and immediately creates a simulated reply.

Implemented now:

- server-configured channels such as `jack:discord` and `jack:sms`
- HTTPS with bearer-token authentication
- durable messages and idempotent send commands
- cursor-based bootstrap and incremental synchronization
- Android channel list, conversation view, text entry, and two-second polling
- Android key/scan-code logging for continued TCL button discovery

Images, reactions, receipts, FCM, WebSockets, platform adapters, and WebRTC are
not implemented yet.

## 1. Configure the server

Requirements: Go 1.18 or newer. HTTPS is the default and requires a certificate
valid for the hostname or IP used by the phone.

```sh
cd server
cp config.example.json config.json
```

Edit `config.json`:

- replace `api_token` with a long random value (`openssl rand -hex 32`)
- set the desired channel entries
- set `tls_cert` and `tls_key` to the certificate files

For normal use, prefer a DNS name and publicly trusted certificate. For LAN
development, `mkcert` can create a certificate for the server hostname/IP. Its
root CA must also be installed as a user CA on the TCL. Debug builds trust user
CAs; release builds trust only system CAs.

For temporary trusted-LAN development without TLS, set `allow_http` to `true`,
change `listen` to `:8080`, and use an `http://` client URL. Only debug APKs
permit cleartext HTTP; release builds still require HTTPS.

Start the server from the `server` directory:

```sh
go run ./cmd/loopback -config config.json
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

## 2. Build and install the Android client

The server URL and matching token are compiled into this prototype APK:

```sh
export ANDROID_HOME="${ANDROID_HOME:-$HOME/Android/Sdk}"
./gradlew \
  -PserverUrl=https://YOUR_SERVER:8443 \
  -PapiToken=YOUR_TOKEN \
  assembleDebug
adb install -r app/build/outputs/apk/debug/app-debug.apk
adb shell am start -n com.example.tclflipkeytester/.MainActivity
```

If these Gradle properties are omitted, the app intentionally points at the
invalid hostname `flip-server.invalid`.

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
to 200 committed `message.created` events after that cursor.

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
