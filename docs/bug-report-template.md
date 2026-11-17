# 缺陷报告模板

[English](bug-report-template.en.md) | 简体中文

复制本模板到 `results/runs/<case-id>/bug.md` 或外部缺陷系统。

```markdown
# [组件] 简短现象

## 环境

- spark-pudding 分支/commit：
- 发行包路径：
- JDK：
- Spark：
- Docker 镜像 tag：
- 用例 ID：
- 数据档位：smoke / scale（行数）

## 配置

- job 文件：`config/jobs/<case-id>.yaml`
- env 文件：`config/env/<name>.properties`
- 是否使用 connection/catalog：
- 是否使用 secret 引用：

## 复现步骤

1.
2.
3.

最小复现命令：

```shell
```

## 期望结果

## 实际结果

## 证据

- 结果目录：`results/runs/<case-id>/`
- 关键日志：
- 目标端查询结果：
- checkpoint/state 状态：

## 影响

- ⬜ 数据丢失
- ⬜ 数据重复
- ⬜ 结果错误
- ⬜ 静默成功
- ⬜ 恢复失败
- ⬜ 凭据泄露
- ⬜ 性能/资源
- ⬜ 仅文档/示例

## 严重级别

P0 / P1 / P2 / P3

## 初步定位

core / source / sink / transform / runtime / client / docs

## 修复后回归

- smoke 用例：
- scale 用例：
- 新增断言：
```

## 严重级别定义

| 级别 | 定义 | 示例 |
| --- | --- | --- |
| P0 | 数据丢失、重复、错误结果、静默成功、凭据泄露 | upsert 覆盖错值；失败但退出码 0 |
| P1 | 功能不可用、恢复失败、明确报错但无法继续 | CDC 无法启动；checkpoint 无法恢复 |
| P2 | 边界行为、错误信息、文档/示例问题 | 示例缺少必填字段；提示信息误导 |
| P3 | 体验、性能、非阻塞告警 | 日志过多、指标缺失 |
