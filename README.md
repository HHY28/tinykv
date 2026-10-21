# TinyKV

TinyKV 是一个用 C++20 编写的分布式键值存储，包含 TCP 服务、并发请求处理、分片存储、日志持久化和基于 Raft 的多节点复制。代码适合用来了解这些组件如何协作，也可以在此基础上继续扩展。

## 功能

- 使用 Linux `epoll` 管理客户端连接，并通过有界线程池处理命令。
- 将命令解析、网络服务、任务调度、KV 存储和 Raft 逻辑拆分为独立模块。
- 将键值数据分为 16 个分片，减少并发访问时的锁竞争。
- 使用 WAL 保存日志，支持快照和进程重启后的数据恢复。
- 实现 Raft 的选举、投票、日志复制和多数派提交。
- 限制客户端连接数和任务队列长度；达到上限时返回 `BUSY`。

## 构建

网络层依赖 Linux `epoll`，可以在 Ubuntu 或 WSL 中构建：

```bash
sudo apt update
sudo apt install -y build-essential cmake
cmake -S . -B build
cmake --build build -j
```

## 运行三个节点

分别在三个终端启动节点。每个节点使用自己的端口和数据目录，并配置另外两个节点：

```bash
# 终端 1
./build/kv_server --id 1 --port 8001 --data data-1 \
  --peer 2@127.0.0.1:8002 --peer 3@127.0.0.1:8003
```

```bash
# 终端 2
./build/kv_server --id 2 --port 8002 --data data-2 \
  --peer 1@127.0.0.1:8001 --peer 3@127.0.0.1:8003
```

```bash
# 终端 3
./build/kv_server --id 3 --port 8003 --data data-3 \
  --peer 1@127.0.0.1:8001 --peer 2@127.0.0.1:8002
```

启动后，用 `STATUS` 查看节点角色：

```bash
nc 127.0.0.1 8001
STATUS
```

等待选举完成后，在 Leader 节点上执行写入。当前客户端请求不会自动转发：向 Follower 发送 `SET` 或 `DEL` 会收到 `NOT_LEADER <leader-id>`，需要连接到 Leader 重试。

## 命令

协议采用每行一条命令的文本格式。value 可以包含空格，不能包含换行：

```text
PING
SET name Alice Example
GET name
DEL name
STATUS
SAVE
QUIT
```

示例响应：

```text
PONG
OK
VALUE Alice Example
OK
STATUS id=2 role=leader term=1 leader=2 commit=1 last_log=1
OK SNAPSHOT
BYE
```

该协议不是 Redis RESP，不能直接使用 `redis-cli` 连接。

## 数据恢复

每个节点的数据目录应独立使用，包含以下文件：

- `raft.log`：追加保存的 Raft 日志。日志项会先写入磁盘，再参与复制。
- `raft.state`：保存当前任期、投票对象和已提交日志位置。
- `snapshot.dat`：保存已应用的 KV 状态。可通过 `SAVE` 手动生成，也会定期生成。

重启节点时，程序从快照和日志恢复状态。当前快照不会截断或压缩 Raft 日志，因此 `raft.log` 会随运行时间增长。

## 并发设置

默认使用 4 个业务线程和最多 1024 个排队任务；Raft RPC 使用 2 个工作线程，内部 RPC 队列上限为 128。可以通过启动参数调整业务线程数和队列上限：

```bash
./build/kv_server --threads 8 --queue 2048
```

Reactor 最多接收 2048 个客户端连接。单个连接同一时间处理一条命令，以保持响应顺序；队列满时返回 `BUSY`。

## 当前范围

这是一个用于学习和实验的轻量实现，Raft 和存储功能尚未覆盖完整生产场景：

- 读请求只在本地 Leader 执行，尚未实现 ReadIndex 或租约读。
- 尚未支持成员变更、预投票、快照安装和日志压缩。
- 节点身份与 peer 列表需要手动配置；运行示例面向同一台 Linux 主机。
- 客户端需要自行连接 Leader；节点间 RPC 使用带超时的阻塞 TCP 调用。
- WAL 有 `fsync`；状态文件和快照通过临时文件重命名保存，尚未实现校验和。
