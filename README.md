# Telegram 短剧处理

从已确认的 Telegram 来源收集视频信息，先按整部剧筛选，再交给现有下载、校验、上传和 AI 信息补全流程。代码按用途分组；下载通道按整部剧推进，剧内共享全局视频并发。视频下载、校验、上传和缓存清理内核保持原实现。

## 从哪里开始

| 要做的事 | 入口 |
|---|---|
| 登录、确认来源、收集源视频信息 | `archive.py` |
| 查看筛选结果、运行现有归档与 AI 流程 | `pipeline.py` |
| 单独采集或检查 Telegram 话题元数据 | `topic_metadata.py` |
| 找到具体代码或辅助脚本 | [项目结构](docs/STRUCTURE.md) |
| 查看参数、质量标准和运行边界 | [流程与操作](docs/PIPELINE.md) |

三个根目录入口保留原命令和参数，实际实现已按职责放进 `video_archive/`。

## 当前流程：先筛选，再按整剧下载

以下命令均从仓库根目录执行，并使用已安装依赖的 Python 环境。完成既有登录与来源白名单确认后，按顺序执行：

```sh
# 仅获取信息，不下载视频或封面
python archive.py scan

# 离线判断整部剧，保存每部剧的结论与完整成员清单
python pipeline.py plan --minimum-quality 720 --report archive/drama-quality-plan.json
```

一部剧以 Telegram 原始话题在扫描边界内可访问的全部视频为准。信息不完整的剧暂缓；包含横屏或方屏的剧排除；其余按既定分辨率和码率标准判断。合格剧的成员完整保留，少量画质异常只影响评分。具体规则见 [整剧筛选说明](docs/PIPELINE.md)。

`plan/batch/run/retry/repair` 都经过同一筛选关口。确认计划后，可限制本次只处理一部剧：

```sh
python pipeline.py batch --drama-limit 1 --minimum-quality 720
```

每个剧通道会完成当前剧的传输、有界补齐和最终判定，再接下一部。`--drama-concurrency` 默认 1；`--concurrency` 默认 4，是所有剧共享的视频并发上限，不会随剧通道数相乘。单段默认 100、最多 500 个视频，分段不改变整剧单位。

默认初次处理后最多再补齐 4 轮，仍受数据库累计最多 5 次尝试等约束。全部预期视频都已验证并提交为 `complete`；最终缺失率严格小于 10% 可判 `accepted_with_gaps`；达到 10% 则拒绝。硬停止或中断记为 `pending`，不能提前按缺失率放行。拒绝或待定的剧不进入后续 AI/转码。画质评分中的“低质异常最多 10%”与这里的下载缺失率是两套独立规则。

已有资料可直接审计并补齐，不需要先找旧报告：

```sh
python pipeline.py repair
```

`repair` 根据完整源清单和已验证的云端提交重算缺口，续传 partial，只补缺失；已齐的剧无需重下载。完整清单、逐视频失败及重试历史和最终结果持续保留，结果存于 `archive/pipeline/projects/<project_ref>/drama-outcomes.json`。后续 AI/转码只接收已判定通过且与当前清单一致的剧。验证完成不要求整剧同时保存在本地缓存，也不会因为剧被拒绝而主动删除已上传的云端对象。

`--only/--first` 指定任一集会扩展到整部剧；`--limit` 是视频数量软目标，当前剧完整纳入后才停止接新剧；`--drama-limit` 直接限制剧数。旧 `archive.py sample/backfill/resume/retry` 保留本地归档用途，不套用新筛选和整剧调度。

## 目录

```text
telegram-video-archive/
├── archive.py / pipeline.py / topic_metadata.py  # 兼容原命令的入口
├── video_archive/                              # 业务代码
│   ├── core.py / paths.py                      # 共享底层与项目路径
│   ├── telegram/                              # Telegram 采集
│   ├── screening/                             # 源完整性与整剧筛选
│   └── pipeline/                              # 整剧调度、存储、AI 流水线
├── scripts/                                   # 维护、诊断和验收脚本
├── config/examples/                           # 不含真实密钥的配置示例
├── prompts/                                   # AI 提示词
├── migrations/                                # 数据库迁移，顺序不变
├── tests/                                     # 回归测试
├── docs/                                      # 操作说明、设计和历史记录
├── .private/                                  # 本机凭证与运行配置（不提交）
└── archive/                                   # 本机状态、报告与缓存（不提交）
```

配置示例见 [config/examples](config/examples)。真实配置、会话和运行数据仍使用原来的 `.private/`、`archive/`，不随源码迁移；不要把密钥写进示例或提交仓库。

## 文档

- [项目结构与脚本索引](docs/STRUCTURE.md)
- [当前流程、筛选规则与操作](docs/PIPELINE.md)
- [双流程与 Supabase 设计](docs/SUPABASE_PIPELINE_DESIGN.md)
- [本地归档历史说明](docs/history/LOCAL_ARCHIVE.md)
- [实施与验收历史记录](docs/history/IMPLEMENTATION_RESULTS.md)
- [历史交接记录](docs/history/PROGRESS_HANDOFF.md)

历史文档保留当时的绝对路径和运行事实，不代表本次已启动生产任务。

## 测试

使用已安装项目依赖的 Python 环境，并提供 FFmpeg/ffprobe：

```sh
python -m unittest discover -s tests -v
```

离线测试不会登录 Telegram 或启动生产下载。需要数据库或历史本地资料的测试，在缺少对应环境时明确跳过。
