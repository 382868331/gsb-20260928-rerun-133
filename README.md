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
- `POST /sessions/{id}/attach`：`{"client":"alice"}`，登记或重连客户端，返回 `token`、`role` 与当前快照。
- `POST /sessions/{id}/release`：`{"token":"..."}`，控制者释放执行控制权。
- `POST /sessions/{id}/claim`：`{"token":"..."}`，控制权空闲时由观察者接管。

## 重连与控制权

- 客户端以自定义 id `attach`；同一 id 重连会拿回同一 `token` 与角色，并立即获得当前暂停或终止快照（含 `reason` 与 `lastPause`）。
- 首个 attach 的客户端成为控制者（`controller`），其余为只读观察者（`observer`）；快照中的 `controller` 字段标明当前归属。
- 观察者可以继续查询快照、变量与等待事件；`continue`/`pause`/`breakpoints` 只能由控制者凭 `token` 执行，否则返回 `403`。
- 控制者 `release` 后控制权悬空，推进请求返回 `409`，任何已 attach 的客户端可 `claim` 接管；控制者断开、程序退出与新客户端接管交错时，终止原因与最后暂停始终保留。
- 未 attach 任何客户端时保持兼容的开放模式（不要求令牌）。

## 幂等请求

- `continue`/`pause` 接受 `requestId`：相同 `requestId` 的重发（如重连后重试）返回首次处理结果并带 `duplicate:true`，不会再次推进程序；已记录的拒绝（如旧 `pauseId`）也会以 `duplicate:true` 解释。

## 并发语义

- 会话请求在单事件循环中串行化，继续执行与读取变量不会拼成混合快照。
- 每个暂停有独立 `pauseId`；旧暂停的继续或读变量不能操作新暂停。
- 重复继续在运行中返回 `409 Conflict`，终止后返回 `410 Gone`。
- 断点修改先更新会话权威集合，再由执行器在指令边界读取。
- 执行器退出（含异常）不会销毁会话事件循环：退出前已入队与退出后新到达的请求都会得到明确结果（终止快照或 `410 Gone`），不会留在无人消费的队列里；只有服务端关闭会话时才回收循环，且会等所有已受理请求处理完。
- 终止快照通过 `reason` 返回终止原因（如 `exception`/`exit`），通过 `lastPause` 返回最后一次暂停（含 `pauseId`、位置与变量）；旧 `pauseId` 始终不能读取新暂停或终止后的状态。
- 虚拟程序 `raise` 会异常终止并返回 `reason=exception`，用于验证所有等待都被结束。

## 测试

```bash
go test -race ./...
```
