# Neutrinonet TCP Transfer

[English](README.md) | [简体中文](README.zh-CN.md)

This is a small Go TCP transport program. Each instance starts two lines at
the same time: one line listens for incoming messages, and one line connects to
the peer for outgoing messages.

## Run

Start the first instance in one terminal:

```powershell
go run . -listen 127.0.0.1:9000 -peer 127.0.0.1:9001
```

Start the second instance in another terminal:

```powershell
go run . -listen 127.0.0.1:9001 -peer 127.0.0.1:9000
```

Type messages in either terminal and press Enter to send them. Incoming messages
are received at the same time. Type `/quit` to close both lines.
