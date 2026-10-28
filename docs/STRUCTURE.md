# 项目结构与脚本索引

代码按职责分组，下载队列新增整剧依次调度；视频下载、上传、校验和缓存清理内核沿用原实现。运行配置、会话、数据目录及数据库迁移的位置不变。

以下命令均从仓库根目录执行，并使用已安装项目依赖的 Python 环境。

## 三个日常入口

| 根目录入口 | 实际实现 | 用途 |
|---|---|---|
| `archive.py` | `video_archive/telegram/archive.py` | 登录、确认来源、源信息扫描及原本地归档 |
| `pipeline.py` | `video_archive/pipeline/runner.py` | 离线计划、归档、迁移、存储检查、AI 和转码入口 |
| `topic_metadata.py` | `video_archive/telegram/topics.py` | 独立话题目录和消息元数据采集 |

原来的 `python archive.py ...`、`python pipeline.py ...`、`python topic_metadata.py ...` 命令继续可用。根入口只负责转交调用，业务实现集中在下面的目录。

```sh
python archive.py --help
python pipeline.py --help
python topic_metadata.py --help
```

## 业务代码：`video_archive/`

| 文件或目录 | 职责 |
|---|---|
| `paths.py` | 固定项目根目录，防止源码搬动后配置、资源和输出定位改变 |
| `core.py` | 消息与视频元数据、本地状态、文件命名和媒体校验等共享底层 |
| `telegram/archive.py` | Telegram 连接、来源确认、扫描与原下载实现 |
| `telegram/topics.py` | 话题目录、分页采集、消息归属与采集审计 |
| `screening/inventory.py` | 从源话题收齐视频成员及完整性证据 |
| `screening/quality.py` | 用完整成员清单判断方向、分辨率、码率和整剧画质档位 |
| `pipeline/runner.py` | 主流程命令和现有任务接线 |
| `pipeline/common.py` | 配置校验、稳定身份、快照、通用错误与数据转换 |
| `pipeline/batch.py` | 整剧计划、剧通道、全局视频并发、分段、补齐和最终判定 |
| `pipeline/completion.py` | 源视频清单、逐集收据核对、缺集判定和中断报告恢复 |
| `pipeline/outcomes.py` | 保存整剧交付结果，按同一份源清单限制后续 AI 与转码准入 |
| `pipeline/cache.py` | 有界本地缓存、预留、锁与清理记录 |
| `pipeline/db.py` | 数据库访问、事务函数和任务租约 |
| `pipeline/storage.py` | 对象存储、分块上传与远端读回验证 |
| `pipeline/ai.py` | 抽帧、模型调用、提示词和用量记录 |
| `pipeline/transcode.py` | 现有转码处理 |

主流程是：**收齐源话题信息 → 整剧画质筛选 → 合格剧整组计划 → 每个剧通道完成传输、补齐及判定 → 通过的剧进入原 AI 与转码处理**。画质筛选始终保留合格剧的全部成员，少量低质异常只影响评分。

下载计划、执行和重试统一使用整剧流程。每个剧通道占用至该剧传输、有界重试和最终判定结束，不能中途换剧。`--drama-concurrency` 默认 1，允许多个剧通道；`--concurrency` 默认 4，仍是全部通道共享的视频任务上限，不相乘。单段默认 100、最多 500 个视频。

默认初次处理后最多做 4 轮补齐，数据库累计最多 5 次尝试的限制仍有效。租约、次数上限或硬性大小约束不能通过重置状态绕过；失败原因和每次重试历史保留。最终以完整源清单为分母：全部视频已验证提交为 `complete`，尚未验证的唯一视频比例严格小于 10% 为 `accepted_with_gaps`，否则拒绝。硬停止或中断为 `pending`。下载缺失率与画质评分的 10% 异常预算分别计算。

`--only/--first` 扩展到整剧；`--limit` 是视频数量软目标，当前剧完整纳入后才停止接新剧；`--drama-limit N` 直接限制本次剧数。视频验证指下载验证、上传读回且云端提交完成，不要求所有集同时占据本地缓存。封面完成另行记录，缺失率按预期视频计算。

`repair` 根据源完整清单和已验证云提交重算缺口，无需旧 report；已齐的旧数据只做审计，无需重新下载，缺失成员和 partial 才进入补齐。

按整剧处理一部的入口示例：

```sh
python pipeline.py batch --drama-limit 1

# 对已有数据重算缺口，只补缺失，不需要旧报告
python pipeline.py repair
```

以上是使用方式，目录整理和测试不等于已经运行真实 Telegram 下载。

## 辅助脚本：`scripts/`

辅助脚本按目的分组，通过新位置调用；三个日常根入口之外的脚本已从根目录迁入这里。

| 新位置 | 用途 |
|---|---|
| `scripts/maintenance/backfill_video_dimensions.py` | 为已有视频记录补录宽高与码率 |
| `scripts/diagnostics/probe_download.py` | 显式单文件 Telegram 下载探测 |
| `scripts/diagnostics/probe_video_input.py` | 检查模型的视频输入方式 |
| `scripts/diagnostics/debug_ai_raw.py` | 检查 AI 返回与解析 |
| `scripts/diagnostics/list_models.py` | 查询现有 AI 配置对应的模型列表，沿用在项目根目录执行的方式 |
| `scripts/validation/validate_implementation.py` | 运行离线回归并保存验收报告 |
| `scripts/validation/validate_topic_metadata.py` | 验证话题采集结果和审计资料 |

查看维护脚本或诊断脚本参数的示例：

```sh
python scripts/maintenance/backfill_video_dimensions.py --help

python scripts/diagnostics/probe_download.py --help
```

离线验收脚本的新调用位置：

```sh
python scripts/validation/validate_implementation.py
```

诊断和维护脚本保留原有用途；其中读取网络、下载探测或补写数据库的操作，只有显式运行对应脚本才发生。目录整理不会自动执行这些操作。

## 配置、提示词与数据

| 位置 | 内容与约定 |
|---|---|
| `config/examples/config.example.json` | Telegram 配置示例 |
| `config/examples/pipeline.config.example.json` | 归档流水线配置示例 |
| `config/examples/pipeline.ai.config.example.json` | AI 配置示例 |
| `prompts/prompt.md` | 原 AI 系统提示词，内容不变 |
| `.private/` | 原本机凭证、会话、白名单、真实配置与锁，位置不变，不提交 |
| `archive/` | 扫描状态、完整清单、逐视频报告、下载文件和缓存，位置不变，不提交 |
| `archive/pipeline/projects/<project_ref>/drama-outcomes.json` | 持久化整剧最终判定；AI/转码核对通过状态和清单一致性 |
| `migrations/` | 原 SQL 迁移，文件内容和执行顺序不变 |
| `tests/` | 原回归测试及整剧调度检查 |
| `requirements.lock` | 原依赖版本锁定文件 |

拒绝或 `pending` 的剧不进入后续 AI 或转码，已上传的云端对象不会因此被主动删除。`ai` 和 `transcode-backlog` 读取持久化结果，只选择已经判定通过且与当前完整清单一致的剧；从未评定的旧数据先运行 `repair` 审计。

配置模板和提示词只移动位置，没有修改内容。三个日常入口仍以项目根目录定位 `.private/` 和 `archive/`，从其他工作目录使用绝对入口路径也不会把它们写到源码目录。

## 文档

- [README](../README.md)：日常入口与当前流程。
- [PIPELINE](PIPELINE.md)：操作参数、整剧筛选规则和流程边界。
- [Supabase 设计](SUPABASE_PIPELINE_DESIGN.md)：双流程与存储、数据库设计。
- [本地归档历史](history/LOCAL_ARCHIVE.md)：原 README 中的本地归档说明与历史验收。
- [实施记录](history/IMPLEMENTATION_RESULTS.md)、[交接记录](history/PROGRESS_HANDOFF.md)：历史运行事实。

历史记录正文中的旧机器路径保留，用来还原当时的执行环境；可点击的项目文件链接已指向本次整理后的文件。历史生产启动授权不表示本次已经执行生产操作。
