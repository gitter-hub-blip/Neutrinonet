# Neutrinonet TCP Transfer

[English](README.md) | [简体中文](README.zh-CN.md)

这是一个小型 Go TCP 传输程序。每个实例会同时启动两条线路：
一条线路监听传入消息，另一条线路连接到对端并发送传出消息。

## 运行

在一个终端中启动第一个实例：

```powershell
go run . -listen 127.0.0.1:9000 -peer 127.0.0.1:9001
```

在另一个终端中启动第二个实例：

```powershell
go run . -listen 127.0.0.1:9001 -peer 127.0.0.1:9000
```

在任意终端中输入消息并按 Enter 即可发送。程序会同时接收传入消息。
输入 `/quit` 可以关闭两条线路。
