# Telegram 短剧处理

从已确认的 Telegram 来源收集完整视频信息，按整部剧筛选、下载补齐与验收，再为合格剧生成 AI 信息并写回数据库，完成实际存在的后处理任务后封存。代码按用途分组，剧内共享全局视频并发；视频下载、校验、上传和缓存清理沿用现有内核。

## 从哪里开始

| 要做的事 | 入口 |
|---|---|
| 登录、确认来源、收集源视频信息 | `archive.py` |
| 一次运行完整流程，或查看计划、处理池与分阶段操作 | `pipeline.py` |
| 单独采集或检查 Telegram 话题元数据 | `topic_metadata.py` |
| 找到具体代码或辅助脚本 | [项目结构](docs/STRUCTURE.md) |
| 查看参数、质量标准和运行边界 | [流程与操作](docs/PIPELINE.md) |

三个根目录入口保留原命令和参数，实际实现已按职责放进 `video_archive/`。

## 当前流程：一个 `run` 完成全链路

以下命令均从仓库根目录执行，并使用已安装依赖的 Python 环境。完成既有 Telegram 登录、来源白名单及数据库、存储、AI 配置后执行：

```sh
python pipeline.py run --drama-concurrency 2 --concurrency 4
```

默认依次进行：**完整源元数据扫描 → 整剧画质筛选 → 按剧下载补齐、上传验收 → 合格剧 AI 生成并事务回写 → 处理已有 faststart 待办 → 适用阶段完成后封存**。源扫描只读取信息，不下载视频或封面。faststart 仅处理已有待办；新视频没有转码任务时算不适用，不会新增转换任务。需要复用已有有效完整扫描时，显式使用 `run --skip-scan`。

一部剧以 Telegram 原始话题在扫描边界内可访问的全部视频为准。信息不完整的剧暂缓；包含横屏或方屏的剧排除；其余按既定分辨率和码率标准判断。合格剧的成员完整保留，少量画质异常只影响评分。具体规则见 [整剧筛选说明](docs/PIPELINE.md)。

手动检查或分阶段操作仍可使用：

```sh
# 仅扫描源信息
python archive.py scan
# 纯离线质量预览，不读取已处理池或云端
python pipeline.py plan --minimum-quality 720 --report archive/drama-quality-plan.json
# 只处理一部剧的下载与验收阶段
python pipeline.py batch --drama-limit 1 --minimum-quality 720
```

每个剧通道会完成当前剧的传输、有界补齐和最终判定，再接下一部。`--drama-concurrency` 默认 1；`--concurrency` 默认 4，是所有剧共享的视频并发上限，不会随剧通道数相乘。单段默认 100、最多 500 个视频，分段不改变整剧单位。

默认初次处理后最多再补齐 4 轮，仍受数据库累计最多 5 次尝试等约束。全部预期视频都已验证并提交为 `complete`；最终缺失率严格小于 10% 可判 `accepted_with_gaps`；达到 10% 则拒绝。硬停止或中断记为 `pending`，不能提前按缺失率放行。拒绝或待定的剧不进入后续 AI/转码。画质评分中的“低质异常最多 10%”与这里的下载缺失率是两套独立规则。

已有资料可直接审计并补齐，不需要先找旧报告：

```sh
python pipeline.py repair
```

`repair` 根据完整源清单和已验证的云端提交重算缺口，续传 partial，只补缺失；已齐的剧无需重下载。完整清单、逐视频失败及重试历史和最终结果持续保留，结果存于 `archive/pipeline/projects/<project_ref>/drama-outcomes.json`。后续 AI/转码只接收已判定通过且与当前清单一致的剧。验证完成不要求整剧同时保存在本地缓存，也不会因为剧被拒绝而主动删除已上传的云端对象。

## 已处理池：避免重复判断和重复付费

当前项目的持久池位于 `archive/pipeline/projects/<project_ref>/processed-dramas.json`。明确画质拒绝（`screening_rejected`）、下载最终拒绝（`download_rejected`）、全阶段完成（`completed`）三类结果入池；临时断网、信息不足、预算或大小限制暂缓、AI 失败、封面或封装待完成不入池，下次沿已有进度继续。已提交成功的 AI 任务会复用，不重复生成。`completed` 要求下载验收、AI 提交、封面完成或无缩略图，以及实际存在的后处理任务全部完成；没有转码任务不会一直待定。

查看处理池或手动解除跳过：

```sh
python pipeline.py processed
python pipeline.py reopen --drama-id UUID
```

将 `UUID` 换成处理池中的剧 ID，可一次提供多个 ID。池子按来源和原话题识别剧：入池后，该话题即使新增视频也默认跳过，需要 `reopen` 才重新纳入。解除只移除跳过标记，不清空数据库尝试次数、不重置已成功 AI 任务。默认源扫描仍完整发现话题；`run/batch/retry/repair/ai/transcode-backlog` 在处理前排除池内剧。`plan` 仍是纯离线源质量预览，不依赖处理池或云端。

`--only/--first` 指定任一集会扩展到整部剧；`--limit` 是视频数量软目标，当前剧完整纳入后才停止接新剧；`--drama-limit` 直接限制剧数。旧 `archive.py sample/backfill/resume/retry` 保留本地归档用途，不套用新筛选和整剧调度。本轮只实现和测试这些流程，没有执行真实 Telegram 运行或付费 AI 调用。

## 扫描与本地下载审计并行

默认会话沿用 `.private/user.session` 和 `.private/process.lock`。需要与现有扫描并行时，先为下载审计建立独立授权：

```sh
python scripts/diagnostics/create_telegram_session.py --name download-audit --interactive
python scripts/diagnostics/audit_download_completeness.py --session download-audit --output archive/download-audit-本次目录
```

新会话保存在 `.private/sessions/download-audit/user.session`，使用独立锁和不同的授权密钥，不复制正在使用的会话数据库。初始化只读已有授权以接受 Telegram 登录令牌；若要求二步验证，密码仅在本机终端隐藏输入，不写入报告。完成后可重复使用该命名会话。

`archive.py` 也支持把 `--session 名称` 放在子命令前；并行任务必须使用不同的会话名称和数据目录。同名会话仍串行。会话隔离不隔离同账号的 Telegram 限流、本机磁盘或网络带宽。`pipeline.py` 的会话选择未扩展。

本地下载审计可用相同 `--output` 续跑，固定抽样清单不变。启动时保存旧报告和状态快照，重新校验本次已成功的文件后跳过，未完成文件按已有字节续传；旧的连接错误会恢复为待处理，原始错误和累计尝试次数仍保留。每个视频最多 5 次有效尝试，连接中断另记。仅审计入口启用有上限的自动重连；仍有连接错误时保存进度并停止，不将后续剧连续记为清单失败。存在未完成清单核查的剧时，报告状态为 `incomplete`，不能视为完成全部样本。

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
