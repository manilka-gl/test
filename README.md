# Rust ntfy for Windows

A lightweight native Windows desktop client for [ntfy](https://ntfy.sh/), written in Rust.

## Features

- Subscribe to any public or authenticated ntfy topic.
- Receive messages using the ntfy JSON polling API.
- Display incoming messages inside the application and as Windows toast notifications.
- Publish messages with title, priority, and tags.
- Optional HTTP Basic authentication or token-as-password authentication.
- Persist server, topic, username, and publishing preferences locally. Passwords and tokens are never saved.

## Run from source

```powershell
cargo run
```

## Build a release executable

```powershell
cargo build --release
```

The executable is created at `target\release\rust-ntfy.exe`.

## Usage

1. Enter an ntfy server, such as `https://ntfy.sh`.
2. Enter the topic name.
3. Add a username and password/token when the server requires authentication.
4. Select **Subscribe** to receive messages.
5. Enter a message under **Publish**, then select **Send message**.

The initial subscription request retrieves up to ten minutes of recent messages. Subsequent requests continue from the last received message ID.

## Security notes

- Use HTTPS for remote servers.
- Topic names on public ntfy servers may be discoverable; use hard-to-guess topic names or an authenticated server.
- Passwords and tokens remain in memory only and are not written to the settings file.

## License

MIT
