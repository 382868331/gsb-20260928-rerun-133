# 程序调试协议服务

Go 实现的最小调试协议服务。协议层与执行器通过 `Executor` 接口交接，首轮内置确定性虚拟执行器；后续接入其他执行器时无需改变客户端请求语义。

## 运行

```bash
go run ./cmd/debugd -addr 127.0.0.1:8080
./scripts/debug-client.sh
```

也可以监听 Unix Socket：

```bash
go run ./cmd/debugd -addr unix:/tmp/debugd.sock
UNIX_SOCKET=/tmp/debugd.sock BASE_URL=http://debug ./scripts/debug-client.sh
```

## 接口

- `GET /programs`：列出虚拟程序。
- `POST /sessions`：`{"program":"loop|count|raise","breakpoints":[2]}`，创建后停在入口暂停。
- `GET /sessions/{id}`：读取当前快照。
- `POST /sessions/{id}/wait`：等待下一次暂停或终止；执行器退出会释放全部等待。
- `POST /sessions/{id}/continue`：`{"pauseId":1}`，只允许操作当前暂停。
- `POST /sessions/{id}/pause`：运行中请求下一条指令边界暂停。
- `POST /sessions/{id}/breakpoints`：`{"lines":[2]}`，替换完整断点集，在安全位置生效。
- `POST /sessions/{id}/variables`：`{"pauseId":1,"names":["i"]}`，只读取指定暂停的一致快照。

## 并发语义

- 会话请求在单事件循环中串行化，继续执行与读取变量不会拼成混合快照。
- 每个暂停有独立 `pauseId`；旧暂停的继续或读变量不能操作新暂停。
- 重复继续在运行中返回 `409 Conflict`，终止后返回 `410 Gone`。
- 断点修改先更新会话权威集合，再由执行器在指令边界读取。
- 虚拟程序 `raise` 会异常终止并返回 `reason=exception`，用于验证所有等待都被结束。

## 测试

```bash
go test -race ./...
```
