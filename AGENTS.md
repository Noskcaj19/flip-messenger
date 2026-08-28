# Development guide

## Product and target device

- This project targets the TCL Flip 2/T408DL running Android 11 (API 30).
- The physical display is 240x320 at 160 dpi. Keep every screen usable at that
  exact size and with hardware-key navigation; do not assume touch input.
- The long-term product is a naive messaging/calling client. Platform-specific
  behavior and credentials belong on the server, not in the Android app.
- A channel is one identity on one platform, for example `jack:discord` and
  `jack:sms` are separate channels.
- Text loopback is implemented. Images, reactions, receipts, typing, FCM,
  platform adapters, and WebRTC voice are future work. When voice is added, the
  server bridges it and the phone uses one server-facing media protocol.

## Repository layout and ownership

- `app/` is a dependency-free native Android Java client. It uses the platform
  HTTP and JSON APIs rather than AndroidX or third-party networking libraries.
- `server/` is a Go modular-monolith loopback server. Its module intentionally
  remains compatible with Go 1.18 even if the local toolchain is newer.
- `server/config.example.json` documents committed configuration.
- `server/config.json`, `server/data/`, and `server/certs/` are ignored local
  state. Never commit their API token, state, private keys, or certificates.
- The Android application/package ID remains
  `com.example.tclflipkeytester` for upgrade compatibility, despite the current
  launcher label being Flip Messenger.

## Current protocol invariants

- `GET /v1/bootstrap` returns configured channels, current messages, and a
  cursor. `GET /v1/sync?after=<cursor>` returns durable events after it.
- `POST /v1/messages` accepts a versioned `message.send` command.
- Every command has an immutable `command_id`; every outgoing message has a
  `client_message_id`. Retrying the same command ID and body must return the
  original result. Reusing an ID with a different body must fail.
- Cursors are monotonically increasing server commit order, not conversation
  timestamp order.
- An accepted send means durably stored by this server. It must not be treated
  as upstream delivery.
- The current loopback transaction stores the outgoing message and an immediate
  simulated channel reply, producing two `message.created` events.
- Server state currently uses mutex-protected atomic JSON-file replacement.
  Preserve the storage boundary so a later SQLite implementation does not
  change the HTTP contract.
- Keep platform adapters behind the server. Do not add Matrix, Discord, SMS, or
  agent fields to the client protocol unless they represent a genuine shared
  capability.

## Local development environment

- The current workstation LAN address is `10.0.0.137`; verify it with
  `ip -4 -brief address show wlan0` because DHCP may change it.
- The active development setup uses `http://10.0.0.137:8080` with
  `allow_http: true` in ignored `server/config.json`.
- Cleartext is temporary LAN-only behavior. It is enabled by
  `app/src/debug/res/xml/network_security_config.xml`; release builds continue
  to reject HTTP. Do not weaken the main network security configuration.
- The client endpoint and token are compile-time Gradle properties. Omitting
  them intentionally builds an APK pointed at an invalid hostname.
- Start the local server from `server/`:

  ```sh
  go run ./cmd/loopback -config config.json
  ```

- Build the current LAN debug APK from the repository root without printing the
  token:

  ```sh
  token=$(python -c 'import json; print(json.load(open("server/config.json"))["api_token"])')
  ANDROID_HOME="$HOME/Android/Sdk" ./gradlew \
    -PserverUrl=http://10.0.0.137:8080 \
    -PapiToken="$token" \
    assembleDebug
  ```

- Install and launch on the attached phone:

  ```sh
  adb install -r app/build/outputs/apk/debug/app-debug.apk
  adb shell am start -n com.example.tclflipkeytester/.MainActivity
  ```

## Verification

- Run server checks from `server/`:

  ```sh
  gofmt -w server.go server_test.go cmd/loopback/main.go
  go test ./...
  go vet ./...
  ```

- Run Android checks from the repository root:

  ```sh
  ANDROID_HOME="$HOME/Android/Sdk" ./gradlew assembleDebug lintDebug
  ```

- Verify the running local server with `curl http://10.0.0.137:8080/healthz`.
- Android key events are logged under `FlipMessenger`; inspect them with
  `adb logcat -s FlipMessenger:I '*:S'`.
- If firmware consumes a hardware button before the app receives it, use
  `adb shell getevent -lt` to identify its raw Linux input event.
- The known working end-to-end smoke test is: bootstrap displays three config
  channels, send `Hey` in Jack (Discord), and observe
  `Discord echo: Hey` with the client cursor advancing by two.

## Keypad behavior

- Channel list: D-pad up/down selects; D-pad center, Enter, or right soft key
  opens.
- Conversation: keypad/IME composes; D-pad center sends; Back or left soft key
  returns to channels.
- Call is consumed and reports that voice is not implemented, preventing the
  system dialer from opening.
- Continue logging Android key code and Linux scan code for every delivered key
  event. Do not remove this diagnostic path while TCL key mapping is unsettled.
- The system IME currently owns multi-tap/T9 behavior. A custom T9 composer is
  explicitly deferred.
