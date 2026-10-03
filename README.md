# orb-rollout-probe

把滚动升级的批次、最大不可用比例、探针参数与就绪结果记录成可查询的服务，支持按工作负载和版本回放升级过程并定位中断原因。

## 运行要求

- Go 1.26 或以上
- SQLite（本服务自带存储，不需要外部数据库）

## 构建、测试与启动

```bash
go build ./...
go test ./...
go run .
```

服务默认监听 `127.0.0.1:8080`。可用环境变量覆盖：

| 变量 | 默认值 | 用途 |
|---|---|---|
| `ADDR` | `127.0.0.1:8080` | HTTP 监听地址 |
| `DB_PATH` | `orb-rollout-probe.db` | SQLite 数据库文件路径 |

## 已公开的入口

### `GET /healthz`

返回服务与存储状态。正常时 HTTP 200：

```json
{"status":"ok","database":"ok"}
```

存储不可用时 HTTP 503：

```json
{"error":{"code":"storage_unavailable","message":"database is not available"}}
```

### `POST /v1/rollouts`

登记一次滚动升级。请求体为单个 JSON 对象，字段如下：

| 字段 | 说明 |
|---|---|
| `namespace`、`workload`、`targetVersion` | 非空字符串，三元组唯一确定一条记录 |
| `batchSize` | 正整数，批次总数 |
| `maxUnavailableRatio` | 闭区间 `[0, 1]` 内的数字 |
| `completedBatches` | `0` 到 `batchSize`（含）之间的整数，缺省为 `0` |
| `probe` | 必填，须完整提供 `type`、`path`、`port`（1–65535）、`timeoutSeconds`、`successThreshold`、`failureThreshold`（三个阈值为正整数） |
| `probeResults` | 每项含 `batch`、`ready`、`failedPods`；失败项可带 `reason` |
| `rollback` | 布尔值 |

行为约定：

- 首次有效提交：单事务写入，返回 201，响应体为稳定 `id` 加完整记录。
- 同三元组、内容完全相同的重复提交：不新增也不改写，返回 200 与首次记录（JSON 空白与键顺序不影响内容比较）。
- 同三元组但内容不同：返回 409，`code` 为 `RolloutConflictError`。
- 记录创建后不可修改或删除；服务不提供 PUT/PATCH/DELETE 入口。
- 并发提交由唯一键与事务保证最多一行，读取只看到已提交的快照。

### `GET /v1/rollouts`

以 `namespace`、`workload`、`targetVersion` 三个查询参数精确查询：

- 命中返回 200 与完整记录。
- 无记录返回 404，`code` 为 `RolloutNotFoundError`。
- 任一参数缺失或留空返回 400，`code` 为 `InvalidRolloutInputError`。

## 错误约定

所有错误响应都是单个顶层 `error` 对象，包含 `code` 与 `message` 两个字符串字段；`message` 不包含 SQL、堆栈或文件路径。

新增入口使用的错误码：

| HTTP | code | 场景 |
|---|---|---|
| 400 | `InvalidRolloutInputError` | 提交 JSON 非法、字段缺失/不完整或取值越界；GET 查询参数缺失或留空 |
| 404 | `RolloutNotFoundError` | 三元组无已提交记录 |
| 409 | `RolloutConflictError` | 三元组已存在但提交内容不同 |
