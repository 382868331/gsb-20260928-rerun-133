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
- `POST /sessions/{id}/attach`：`{"clientId":"alice","role":"controller","takeover":false}`，登记客户端并分配角色，返回当前权威快照；断线重连用同一 `clientId` 重新 attach 即可取回暂停或结束状态。
- `POST /sessions/{id}/detach`：`{"clientId":"alice"`}，释放该客户端持有的控制权，程序保持当前暂停不动。
- `POST /sessions/{id}/wait`：等待下一次暂停或终止；执行器退出会释放全部等待。
- `POST /sessions/{id}/continue`：`{"pauseId":1,"clientId":"alice","requestId":"r1"}`，只允许操作当前暂停。
- `POST /sessions/{id}/pause`：`{"clientId":"alice","requestId":"r2"}`，运行中请求下一条指令边界暂停。
- `POST /sessions/{id}/breakpoints`：`{"lines":[2],"clientId":"alice","requestId":"r3"}`，替换完整断点集，在安全位置生效。
- `GET /sessions/{id}/breakpoints`：预览断点版本：当前版本（含每条断点的启停、条件、命中次数与条件诊断）、候选列表与历史版本；observer 也可读取。
- `POST /sessions/{id}/breakpoints/candidates`：`{"breakpoints":[{"line":2,"enabled":true,"condition":"i > 1","hitCount":3}],"clientId":"alice","requestId":"r4"}`，暂存一个可预览的候选版本，记录准备时的暂停与命中基线。
- `POST /sessions/{id}/breakpoints/adopt`：`{"candidateId":1,"clientId":"alice","requestId":"r5"}`，仅当前控制者可采用；新版本从下一次 continue 起生效，返回的 `effectivePauseId` 指明生效起点。
- `POST /sessions/{id}/breakpoints/refresh`：`{"candidateId":1,"clientId":"alice"}`，核对后重新基线化被标记的候选。
- `POST /sessions/{id}/source`：`{"clientId":"alice","validLines":[2,3]}`，报告源码变化；所有待采用候选被标记需要重新核对的位置与命中记录。
- `POST /sessions/{id}/variables`：`{"pauseId":1,"names":["i"]}`，只读取指定暂停的一致快照。

## 并发语义

- 会话请求在单事件循环中串行化，继续执行与读取变量不会拼成混合快照。
- 每个暂停有独立 `pauseId`；旧暂停的继续或读变量不能操作新暂停。
- 控制权唯一：第一个请求控制权的客户端成为 controller，其余为只读 observer；observer 可以查询快照、等待与变量，但 continue/pause/breakpoints 返回 `403`。controller detach 后控制权重置，新客户端可接管；`takeover:true` 可显式从失联 controller 手中夺取控制权。
- 推进类请求支持 `requestId` 幂等：重连后的重复请求返回已存结果并带 `duplicate:true`，不会再次推进程序；进行中的重复请求返回 `409` 表示已在处理。
- 重复继续在运行中返回 `409 Conflict`，终止后返回 `410 Gone`。
- 断点修改先更新会话权威集合，再由执行器在指令边界读取。
- 断点按版本管理：每次采用（或 `POST /breakpoints` 直接替换）生成递增版本号；每个暂停快照带 `breakpointVersion`，可核对本次暂停实际使用的版本。版本被替换时其命中次数归档进历史，重开后可区分已发生的命中与尚未生效的候选修改。
- 候选在准备后遇到新一轮暂停或源码变化会被标记 `recheck`（指出需重新核对的位置与命中记录），被标记的候选不能静默采用（`409`），必须先 refresh 或重新暂存；旧控制者的迟到采用返回 `403`，不会改变当前版本。
- 断点条件支持 `name`、`name == N`、`!=`、`<`、`<=`、`>`、`>=`；条件求值出错会以 `reason=condition-error` 暂停并保留诊断（快照与状态中的 `conditionError`），不计为命中也不当作未命中。
- 断点支持 `hitCount` 次数约束：`hitCount:N` 时条件每通过 N 次才暂停一次（缺省为每次命中都停）。各断点分别累计次数——每次条件通过都计一次命中（包括尚未达到 N、不会暂停的通过）；因此第四次通过触发暂停时，当前状态与历史版本对该断点都累计 4 次，与暂停快照的 `hitCount:4` 互为印证，不会只记成一次。条件求值失败不计为命中也不当作未命中，不受次数约束影响，仍立即以 `condition-error` 暂停并给出诊断。每次暂停的快照带 `hitCount`，记录本次暂停实际依据的累计通过次数，与 `breakpointVersion` 一起可核对版本与次数依据。
- 候选、采用版本与暂停记录使用同一套计数依据：候选暂存时记录当时每条断点的累计通过基线；自基线后发生过新通过（即使未达到次数目标、没有产生暂停）的行在候选的 `recheck.hitLines` 中明确标出，采用前必须 refresh 重新核对或重新暂存。会话在每次暂停（含手动边界暂停与条件错误暂停）后与执行器核对原始通过计数，归档历史版本前也先核对，因此历史 `hits`、当前状态与暂停快照始终一致。
- 执行器侧按断点规格累计次数：规格（启停、条件、次数目标）不变的断点在断点集重装后继续累计；被修改（含改条件、改次数目标）、停用后重启用或新加的断点从零重新计数，被移除的行计数随之丢弃。停用一个断点本身保留其已累计计数（行仍在集合中、规格仍可核对）；改条件或次数目标会使执行器把该断点视为新规格而清零；控制权交接（detach/takeover）不改变断点版本与任何计数，新控制者看到的计数与前任一致。采用新版本时会话侧命中记录先核对再归档进历史，新版本的各断点重新累计；新版本在运行中于安全点生效时，执行器为未改规格断点保留的计数会继续作为该版本计数的起点。
- 执行器退出（含异常）不会销毁会话事件循环：退出前已入队与退出后新到达的请求都会得到明确结果（终止快照或 `410 Gone`），不会留在无人消费的队列里；只有服务端关闭会话时才回收循环，且会等所有已受理请求处理完。
- 终止快照通过 `reason` 返回终止原因（如 `exception`/`exit`），通过 `lastPause` 返回最后一次暂停（含 `pauseId`、位置与变量）；旧 `pauseId` 始终不能读取新暂停或终止后的状态。
- 虚拟程序 `raise` 会异常终止并返回 `reason=exception`，用于验证所有等待都被结束。

## 测试

```bash
go test -race ./...
```
