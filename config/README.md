# config

[English](README.en.md) | 简体中文

测试配置，按用途分组：

- `docker/`：`docker-compose.yml`、PostgreSQL/MySQL 初始化 SQL、Mongo 副本集初始化、
  Kafka/Schema Registry/ES/MinIO/Redis/etcd 配置。
- `env/`：`env.properties`、JDBC/Redis/etcd 状态后端 properties 模板。
- `jobs/`：组件用例和 E2E 用例的 job YAML/JSON/HOCON。
- `schemas/`：表 DDL、Avro/Protobuf schema、Elasticsearch mapping。

约定：

- 连接串、端口、密钥只放在 `env/` 或被 `.gitignore` 排除的本地文件中；
- job 文件使用占位符（如 `${PG_HOST}`）由执行器渲染；
- 所有 job 文件遵循 `job.mode` 和“恰好一个 sink”规则。
