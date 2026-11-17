# config

English | [简体中文](README.md)

Test configuration, grouped by purpose:

- `docker/`: `docker-compose.yml`, PostgreSQL/MySQL init SQL, MongoDB replica-set init,
  Kafka/Schema Registry/ES/MinIO/Redis/etcd configuration.
- `env/`: `env.properties` and JDBC/Redis/etcd state backend property templates.
- `jobs/`: job YAML/JSON/HOCON for component and E2E cases.
- `schemas/`: table DDL, Avro/Protobuf schemas and Elasticsearch mappings.

Conventions:

- Connection strings, ports and secrets live only under `env/` or in local files excluded by
  `.gitignore`;
- Job files use placeholders such as `${PG_HOST}` that the runner substitutes;
- Every job follows `job.mode` and the exactly-one-sink rule.
