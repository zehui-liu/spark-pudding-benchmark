# Docker 服务

[English](README.md) | 简体中文

测试床使用同一个 compose 文件按 profile 拆分，避免笔记本上的 Docker VM 同时运行所有服务。

| Profile | 服务 | 宿主机端口 |
| --- | --- | --- |
| `relational` | PostgreSQL、MySQL、MongoDB | 15432、13306、27018 |
| `streaming` | Kafka、Schema Registry | 19092、18081 |
| `search` | Elasticsearch | 19200 |
| `infra` | MinIO、Redis、etcd | 19000/19001、16379、12379 |
| `hdfs` | NameNode、DataNode | 18020、19870、19866 |

启停方式：

```shell
scripts/services/up.sh relational
scripts/services/down.sh relational
scripts/services/up.sh streaming
scripts/services/down.sh streaming
```

`up.sh` 首次使用时会从 `.env.example` 创建 `config/docker/.env`。
`down.sh <profile>` 只停止该 profile；`down.sh all --volumes` 会删除整个项目及其数据卷。

说明：

- **Seed 数据挂载**：PostgreSQL 和 MySQL 容器把 `data/seed` 只读挂载到 `/data-seed`。
  加载 seed CSV 时使用 `PUDDING_DATA_CONTAINER_DIR=/data-seed`；`data/generated` 仍挂载在
  `/data`。
- **MongoDB** 使用容器端口 27018，使副本集成员 `localhost:27018` 在容器内外都可访问。
  `scripts/services/init-mongo.sh` 在容器启动后初始化 `rs0`。
- **MySQL** 使用 `--secure-file-priv=` 启动，使 `LOAD DATA INFILE` 可以读取只读的
  `/data` 挂载。加载器不写服务端文件。
- **Schema Registry** 使用 `PLAINTEXT://kafka:29092`，发布为 amd64 镜像；在 Apple Silicon
  上需要启用 Docker Desktop Rosetta 模拟。如果镜像不可用，可将 Schema Registry 作为宿主机
  进程运行，并更新 `06_produce_kafka.py` 中的 `--schema-registry`。
- **HDFS** 是可选且实验性的；DataNode 对外声明 `127.0.0.1:19866` 供宿主机 Spark 客户端
  使用。宿主机客户端必须设置 `dfs.client.use.datanode.hostname=true`。命名卷以 root
  创建，而镜像以 `hadoop` 运行，因此首次启动需要：
  `docker compose --profile hdfs run --rm --no-deps --user root --entrypoint
  /bin/sh namenode -c 'mkdir -p /tmp/hadoop-root/dfs/name && chown -R
  hadoop:hadoop /tmp/hadoop-root'`，然后执行
  `docker compose --profile hdfs run --rm --no-deps namenode hdfs namenode
  -format -nonInteractive`。对 DataNode 卷执行同样的归属修复。重启后的 NameNode 会保持
  safe mode 直到 DataNode 上报；运行作业前用 `hdfs dfsadmin -safemode wait` 等待。
- **HDFS 宿主机路径**：`scripts/run/run-hdfs.sh` 会准备
  `results/state/os-03/hadoop-conf`，写入 `fs.defaultFS` 和 DataNode hostname 设置，
  然后验证跨重启的 checkpoint/state。
- **MinIO 镜像**可通过 `.env` 中的 `MINIO_IMAGE` 和 `MINIO_CLIENT_IMAGE` 覆盖（Docker Hub
  不可用时）。服务端镜像必须提供 `/minio/health/live`；客户端镜像必须提供 `mc` 供
  `minio-init` 使用。`infra` profile 同时运行 Redis 和 etcd。etcd 健康检查使用 exec 形式，
  因为该镜像没有 shell。
- `.env.example` 中的所有凭据都是测试专用默认值。

对象存储用例使用 `scripts/run/run-object-store.sh`。runner 通过 `mc admin` 创建两个
受限 MinIO 用户：

| 用户 | 允许的 prefix |
| --- | --- |
| `pudding-state` | `checkpoints/*`、`state/*` |
| `pudding-lake` | `warehouse/os-02/*` |

凭据写入 `results/state/object-store/secrets.properties`，权限为 `0600`；job 文件只包含
`${secret:file:...#key}` 引用。

其他 L6 runner：

| Runner | 用例 |
| --- | --- |
| `scripts/run/run-hdfs.sh` | OS-03 checkpoint/state 和 HDFS 重启 |
| `scripts/run/run-state-backends.sh` | OS-05 JDBC/Redis/etcd state |
| `scripts/run/run-maintenance.sh` | OS-06 表维护 |
| `scripts/run/run-maintenance-server.sh` | OS-07 HTTP 控制面 |
| `scripts/run/run-capabilities.sh` | OS-08 capability manifest |
