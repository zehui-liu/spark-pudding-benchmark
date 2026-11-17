# Docker services

English | [简体中文](README.zh-CN.md)

The test bed uses one compose file split into profiles so a laptop Docker VM
does not have to run every service at once.

| Profile | Services | Host ports |
| --- | --- | --- |
| `relational` | PostgreSQL, MySQL, MongoDB | 15432, 13306, 27018 |
| `streaming` | Kafka, Schema Registry | 19092, 18081 |
| `search` | Elasticsearch | 19200 |
| `infra` | MinIO, Redis, etcd | 19000/19001, 16379, 12379 |
| `hdfs` | NameNode, DataNode | 18020, 19870, 19866 |

Start and stop with:

```shell
scripts/services/up.sh relational
scripts/services/down.sh relational
scripts/services/up.sh streaming
scripts/services/down.sh streaming
```

`up.sh` creates `config/docker/.env` from `.env.example` on first use.
`down.sh <profile>` stops only that profile; `down.sh all --volumes` removes the
whole project and its data volumes.

Notes:

- **Seed data mount**: `data/seed` is mounted read-only at `/data-seed` in the
  PostgreSQL and MySQL containers. Load seed CSVs with
  `PUDDING_DATA_CONTAINER_DIR=/data-seed`; `data/generated` remains at `/data`.
- **MongoDB** starts on container port 27018 so the replica-set member
  `localhost:27018` is reachable both inside the container and from the host.
  `scripts/services/init-mongo.sh` initializes `rs0` after the container starts.
- **MySQL** runs with `--secure-file-priv=` so `LOAD DATA INFILE` can read the
  read-only `/data` mount. The loader does not write server-side files.
- **Schema Registry** uses `PLAINTEXT://kafka:29092` and is published as an
  amd64 image; on Apple Silicon enable Docker Desktop Rosetta emulation. If the
  image is unusable, run Schema Registry as a host process and update
  `--schema-registry` in `06_produce_kafka.py`.
- **HDFS** is optional and experimental; the DataNode advertises
  `127.0.0.1:19866` for host-side Spark clients. Host clients must set
  `dfs.client.use.datanode.hostname=true`. The named volumes are created as
  root, while the image runs as `hadoop`, so the first start needs:
  `docker compose --profile hdfs run --rm --no-deps --user root --entrypoint
  /bin/sh namenode -c 'mkdir -p /tmp/hadoop-root/dfs/name && chown -R
  hadoop:hadoop /tmp/hadoop-root'`, followed by
  `docker compose --profile hdfs run --rm --no-deps namenode hdfs namenode
  -format -nonInteractive`. Apply the same ownership command to the datanode
  volume. A restarted NameNode stays in safe mode until the DataNode reports;
  wait with `hdfs dfsadmin -safemode wait` before running a job.
- **HDFS host paths**: `scripts/run/run-hdfs.sh` prepares
  `results/state/os-03/hadoop-conf` with `fs.defaultFS` and the DataNode
  hostname setting, then exercises checkpoint/state across restarts.
- **MinIO image** can be overridden with `MINIO_IMAGE` and
  `MINIO_CLIENT_IMAGE` (both in `.env`) when Docker Hub is unavailable. The
  server image must expose `/minio/health/live`; the client image must provide
  `mc` for `minio-init`. The `infra` profile also runs Redis and etcd. The etcd
  health check uses exec form because that image has no shell.
- All credentials in `.env.example` are test-only defaults.

Object-storage cases use `scripts/run/run-object-store.sh`. The runner creates
two scoped MinIO users through `mc admin`:

| User | Allowed prefix |
| --- | --- |
| `pudding-state` | `checkpoints/*`, `state/*` |
| `pudding-lake` | `warehouse/os-02/*` |

Their credentials are written to `results/state/object-store/secrets.properties`
with mode `0600`; job files contain only `${secret:file:...#key}` references.

Other L6 runners:

| Runner | Cases |
| --- | --- |
| `scripts/run/run-hdfs.sh` | OS-03 checkpoint/state and HDFS restarts |
| `scripts/run/run-state-backends.sh` | OS-05 JDBC/Redis/etcd state |
| `scripts/run/run-maintenance.sh` | OS-06 table maintenance |
| `scripts/run/run-maintenance-server.sh` | OS-07 HTTP control plane |
| `scripts/run/run-capabilities.sh` | OS-08 capability manifest |
