# 本地归档流程历史记录

以下正文从原 README 完整迁入，保留当时的运行事实、命令和验收范围。当前入口与目录见 [README](/Users/qianpeijia/Documents/ChatGPT/短剧/telegram-video-archive/README.md) 和 [项目结构](/Users/qianpeijia/Documents/ChatGPT/短剧/telegram-video-archive/docs/STRUCTURE.md)。

## 原归档流程与历史说明

独立的命令行工具。只读取用户确认的单个频道/群历史，不监听新消息，不做后续增量，不发送机器人指令，不自动加入群或付费。原 archive.py 保持本地归档；新增 pipeline.py 仅显式执行 Supabase 迁移、清单导入和两个已有样本归档，不自动启动全量。

设计基线：[双流程与 Supabase 存储/表结构](/Users/qianpeijia/Documents/ChatGPT/短剧/telegram-video-archive/docs/SUPABASE_PIPELINE_DESIGN.md)。实现与安全操作说明：[PIPELINE.md](/Users/qianpeijia/Documents/ChatGPT/短剧/telegram-video-archive/docs/PIPELINE.md)。**代码及本机离线/数据库验证已实施，真实 Supabase 验收尚未执行，真实 AI 适配器尚未确定。** 以下历史验收记录不是本次云端验收。

## 当前下载策略（2026-10-20）

- 使用一个进程、内部默认 2 路并发；退出时取消并等待所有下载任务完成清理。
- 经用户确认，来源/消息的 `protected/noforwards` 仅记录为元数据，不再作为本地下载拦截；实际使用普通 Telegram API，服务端拒绝仍记录为失败。
- 论坛 General（`topic_id=1`）不下载，候选视频记为 `skipped`，消息元数据保留。已有目录 893 条中包含 General，排除后为 892 个话题；不再以此数量差异阻塞下载。
- 恢复/全量流程会将旧的保护拦截状态重新置为 pending；`retry` 仍只处理 failed。限时媒体、来源白名单和媒体身份校验仍保留。
- 以下 2026-10-19 验收记录描述当时版本，不代表当前已完成全量下载。

## 环境与依赖

- Python 3.10+；本机已建好 `/Users/qj/python_project/telegram-video-archive/.venv`。
- Telethon 1.45.0，及全部传递 Python 依赖锁定于 `/Users/qj/python_project/telegram-video-archive/requirements.lock`。
- 系统工具 FFmpeg / ffprobe；本机验证版本 9.0.2，Homebrew 安装（不是 Python 依赖）。
- SQLite 是 Python 标准库。数据库只是本地恢复日志，不是未来业务数据库设计。

重建环境：

```sh
python3 -m venv /Users/qj/python_project/telegram-video-archive/.venv
/Users/qj/python_project/telegram-video-archive/.venv/bin/python -m pip install -r /Users/qj/python_project/telegram-video-archive/requirements.lock
HOMEBREW_NO_AUTO_UPDATE=1 /opt/homebrew/bin/brew install ffmpeg
```

## 1. 本机交互登录及确认目标（仅这两步需要人）

在本机 Terminal / VS Code 集成终端运行，不要把验证码、API hash、密码或 session 发给助手：

```sh
/Users/qj/python_project/telegram-video-archive/.venv/bin/python /Users/qj/python_project/telegram-video-archive/archive.py login
/Users/qj/python_project/telegram-video-archive/.venv/bin/python /Users/qj/python_project/telegram-video-archive/archive.py resolve --confirm
```

第一条依次询问 API ID、API hash、国际格式手机号、验证码及可能的两步验证密码。除 API ID 外均隐藏输入。验证码和密码不落盘；API 凭证与 session 保存在 `/Users/qj/python_project/telegram-video-archive/.private/`（目录 0700，文件 0600，已忽略版本控制）。不复用项目外 session。

第二条解析用户提供的邀请链接，显示实际名称、类型、ID、内容保护标志。**只有你在本机输入显示的完整 ID 才写入白名单**；不要仅凭名称确认。不下载机器人私聊。

也可以只解析：

```sh
/Users/qj/python_project/telegram-video-archive/.venv/bin/python /Users/qj/python_project/telegram-video-archive/archive.py resolve
```

若邀请已失效、尚未加入或实际目标不明确，停止并提供准确可访问链接/ID。`resolve --entry '准确链接或ID' --confirm` 可替换入口。绝不自动加入、续费或跳转到相似群。已有白名单不会被不同来源覆盖。Desktop 的登录不等于本工具的 API 登录。

配置格式参考 `/Users/qj/python_project/telegram-video-archive/config.example.json`，不含真实凭证。不要把聊天中出现过的密钥再写入 README、命令行参数或报告。session 等同于账号权限，请用 FileVault 等磁盘加密保护，不要分享整个 `.private` 目录。本工具当前使用直连 MTProto，不自动读取系统 HTTP 代理；网络不可达时会有限重试后退出。

## 2. 小样本 → 全量（同一任务，同一上界）

```sh
/Users/qj/python_project/telegram-video-archive/.venv/bin/python /Users/qj/python_project/telegram-video-archive/archive.py sample
/Users/qj/python_project/telegram-video-archive/.venv/bin/python /Users/qj/python_project/telegram-video-archive/archive.py backfill
```

`sample` **先只扫描完整历史元数据**，再选约 3 个代表性视频下载：优先文件附件、缺少标题/标签/简介、相册或回复关系；不存在的类别不伪造。少于 3 个则验证现有视频。完整元数据扫描能保留关联文字及得出容量估计，不是提前下载全量视频。

`backfill` 在未跑小样本时会自动先跑样本，通过后继续全量；已有通过样本则直接继续。两条命令共用首次任务边界。不会重新扫描新消息。全量执行无需再次确认目标或询问继续。

媒体校验：每个成功文件大小（API 可取得时）、SHA-256、ffprobe 视频流；样本由 ffmpeg **完整解码视频与音频**，不是仅识别扩展名。非样本不声称逐帧解码或人工观看。样本文件也可在本机播放器人工打开复核。

## 3. 恢复 / 失败重试 / 验收

```sh
# Ctrl-C、SIGTERM、网络中断或容量不足后：同一目录、同一上界
/Users/qj/python_project/telegram-video-archive/.venv/bin/python /Users/qj/python_project/telegram-video-archive/archive.py resume

# 仅重试 failed 状态，不重新扫描已完成历史
/Users/qj/python_project/telegram-video-archive/.venv/bin/python /Users/qj/python_project/telegram-video-archive/archive.py retry

# 离线重算成功文件的大小/hash/媒体探测，并完整解码样本
/Users/qj/python_project/telegram-video-archive/.venv/bin/python /Users/qj/python_project/telegram-video-archive/archive.py verify

# 仅重新导出状态及报告，不声称重新校验文件
/Users/qj/python_project/telegram-video-archive/.venv/bin/python /Users/qj/python_project/telegram-video-archive/archive.py report
```

`resume` 包括 pending、downloading、failed 的恢复，并复核 success 后跳过。`retry` 仅处理 failed；遇到安全停止而保持 pending 的项请用 `resume`。不实现字节级续传：`.part` 下次重新下载。成功文件不跨来源消息合并，即使原始名字或 SHA-256 相同，也保留每条消息记录和独立稳定路径。每次命令有项目级锁，不能同时运行登录/下载两个进程。

退出码：全量/恢复/重试/验收/报告只有完整验收才为 0，不完整为 2；中断为 130。`login`、`resolve` 和 `sample` 的 0 只表示该步骤成功，**不表示全量完成**。缺配置会在网络访问前停止。

如磁盘不足，报告包含已知剩余字节、当前可用空间、保留空间及实际缺口。默认保留 512 MiB 给状态与系统；扫描页也检查空间。全量下载前检查全部已知剩余体积；未知大小依赖逐块检查。扫描尚未结束时，体积只是下界，不能据此声称够用。

如需迁移存储，将**整个 archive 目录**复制到新磁盘，再把 `--data /绝对路径` 放在子命令前；不要只搬视频或只删数据库。数据库中的文件路径相对数据目录。不要删除状态以冒充恢复成功。

## 本地输出与格式

默认全部业务数据位于 `/Users/qj/python_project/telegram-video-archive/archive/`：

| 文件/目录 | 作用 |
|---|---|
| `videos/` | `{来源ID}_{消息ID}_{媒体ID}_{安全文件名}`，未完成为 `.part` |
| `messages.jsonl` | **完整导出依据**，每行一个源消息，包括无视频文字、消毒后的 API 消息及 download 对象 |
| `index.csv` | UTF-8 BOM 人工查看版，复杂字段用 JSON 字符串，空列表/未知值留空；危险公式开头加单引号 |
| `topics.json` | 历史 API 响应中可取得的主题元数据，不额外访问其他来源 |
| `state.sqlite3` | 事务恢复依据；含 task/messages/videos/pages/topics；不是业务表设计 |
| `progress.json` | 冻结上界、游标、样本状态、安全停止原因 |
| `pages.jsonl` | 每页请求游标、返回 ID、下一游标及采集时间，供核查分页边界 |
| `report.json` | 计数、容量、时间范围、校验、限制和最终是否完整 |

主索引中的 `schema_version=1`。文本缺失为 `""`、列表为 `[]`、未知数值为 `null`。`source` 含平台来源身份；`message_id/media_id` 与来源共同构成稳定媒体身份。`download` 对无候选视频的消息为 null，否则包含状态、尝试次数、事件、相对路径、大小、SHA-256、探测和采集/下载/验证时间；下载时刷新消息另存 `download_message_snapshot`，不覆盖扫描原文。

不导出整个账号、联系人、其他聊天；不序列化 API 响应中的 users/chats。原始消息递归去除 `access_hash/file_reference` 等访问凭据和二进制预览，保留原文、实体、相册、回复、主题、转发来源、统计及时间。消息原文如果本身含用户发布的链接/敏感信息仍原样保留，分享索引前应自行审查。

标题、简介、集数只提取单独一行明确的 `标题：`/`剧名：`、`简介：`、`集数：`；标签提取明确 `#标签`。不把文件名或正文首行猜成标题，不根据相似标题合并剧集。原始文件名仅作为元数据，落盘名清理路径字符并加入稳定 ID。相册与回复通过 ID 关联所有保留消息；不猜测相邻文字对应哪部剧，不追踪到白名单外来源。

## 边界、筛选与验收解释

- 第一次 sample/backfill 冻结开始时间、**开始时间向下取整到秒的排他日期边界**和该边界前最高消息 ID。Telegram 日期精度为秒，为避免收进任务开始后消息，同一秒的消息全部在边界外（不足一秒保守窗口，报告明确记录）。恢复不变更边界。
- 降序原始 `messages.getHistory` 分页，`offset_id` 排他，下页取本页最小 ID，直到真正返回空页；短页、ID 间隔和界面数量都不是结束依据。页记录与游标一次事务写入，下载状态单独事务。记录并检测重复、越界、顺序异常；异常停止而不是静默忽略。
- 直接文档附件满足 **视频属性 OR video MIME OR 常见视频扩展名**即进入待校验候选，包含普通视频、视频文件和视频格式动画。网页预览/非文档有明确排除原因；没有任何视频证据的文档保留元数据但不盲目下载全部附件。故意伪装到三个信号都缺失的视频无法可靠识别，这是已知检测限制。扩展名候选的媒体探测失败保留为失败，不能算成功。
- 付费媒体、历史迁移、空消息等有解释性限制；不自动跨群追溯迁移历史或购买付费媒体。限时媒体仍会阻止下载；每个实际下载前检查来源身份及媒体 ID/大小，保护标志仅记录。
- 登录后的预检保存实际来源、检查时间与授权状态；不因来源保护标志停止扫描。`download_allowed` 代表本地策略允许尝试，不保证服务端必定允许下载。
- API RPC 超时 60 秒，连接超时 20 秒，每层最多 3 次有限尝试；单文件总下载超时 1800 秒；指数退避。FloodWait 最多单次等待 60 秒，超过保存状态退出；文件引用失效重新读取同一白名单消息，最多 3 次文件尝试。
- 每个识别视频有互斥状态：`pending/downloading/success/failed/blocked/skipped`。`skipped` 为明确排除的 General 候选，不计入待下载容量；`success` 包含此前成功项。**本次操作**另列 `downloaded_success/verified_skipped/failed/blocked`，不要把累计 success 与本次 verified_skipped 相加。样本在同次 backfill 不再算一次 skip。
- `scanned_messages = identified_videos + excluded_nonvideo_messages`；`identified_videos = 所有视频状态之和`。`COMPLETE_VERIFIED` 要求空页结束、分页集合核对、没有待处理/失败/阻止/未解释限制、成功文件验证通过及样本通过。只下载样本一般是 INCOMPLETE。
- 只对当前账号可访问且允许下载、冻结边界内内容负责；Telegram 无历史原子快照，删除/改动/权限变化可能发生。消息被删或媒体身份改变会明确失败，不拿别的文件顶替；不能声称获得频道曾经发布过的一切。
- 故障注入测试使用模拟 Telegram 客户端和真实 FFmpeg 生成的视频；**不等于真实来源登录、下载或全量验收**。真实验收需要本机登录后实际执行。

## 显式单文件 API 探测（独立于回填）

用户撤销“不绕过内容保护”的要求后，可显式运行：

```sh
/Users/qj/python_project/telegram-video-archive/.venv/bin/python /Users/qj/python_project/telegram-video-archive/probe_download.py --attempt-protected
```

此命令保留 `noforwards` 原值，但不因该标志在本地拒绝调用标准用户 API 的 `download_media`。仅使用已登录用户和已确认的单来源白名单，最多读取 10 页（每页最多 100 条）寻找一个非限时视频，只尝试该视频一次；下载限时 600 秒。服务端拒绝时记录错误，不尝试其他权限、来源或转发路径。技术上能够下载不代表拥有转载或分发授权。

输出单独放在 `/Users/qj/python_project/telegram-video-archive/archive/probe-时间戳/`，包括视频、去除访问凭据的 `message.json`、`report.json`；完整下载后必须通过大小、SHA-256、ffprobe 和完整 ffmpeg 解码。`SINGLE_VIDEO_VERIFIED` **仅表示这一文件通过**，不是全量回填成功。未完成文件保留 `.part`，再次运行创建新探测目录，不提供断点恢复。

不会修改原回填 SQLite、进度或总报告；原 `archive.py sample/backfill` 的保护预检仍保留。以上“内容保护会阻止下载”的说明适用于原回填流程，不适用于此显式探测命令。

## 话题目录与短剧索引（纯元数据）

```sh
/Users/qj/python_project/telegram-video-archive/.venv/bin/python /Users/qj/python_project/telegram-video-archive/topic_metadata.py
```

沿用本项目用户 session、单进程锁、白名单与有限 RPC 重试，并额外限定来源 ID
`-1004347610716`。不会下载媒体、加入群、修改群设置、发送消息，亦不写原回填
`/Users/qj/python_project/telegram-video-archive/archive/state.sqlite3`。
`protected` 记录真实值，不把保护标志解释为元数据不可访问。

流程：直接查询话题 6557；两次完整目录遍历（默认每页 100，分别到实际空页）；
只对话题 6557 进行 `GetRepliesRequest` 分页核查（固定采集时间/首次消息 ID 上界）；
刷新消息 6559，对比既有探测的媒体身份，并离线复核已有文件大小、SHA-256、ffprobe。
不会为了查名称重复下载，也不重复完整解码已通过的文件。

每次输出独立目录 `/Users/qj/python_project/telegram-video-archive/archive/topics-时间戳/`：

- `metadata.sqlite3`：复合键 `(source_id, topic_id)` 的目录、逐次名称观测、消息和分页审计。
- `topics.json`、`topic-observations.jsonl`：当前采集名称及原始必要字段，保留改名观测；同名不同 ID 不合并。
- `messages.jsonl`：本次选定话题的消息元数据；`topic_title` 独立于原正文和原 `title`。
- `dramas.json`、`dramas.csv`：话题标题—话题 ID—已观测视频列表。JSON 包含文件路径及既有校验结果。
- `pages.jsonl`：请求游标、响应 ID、计数、排序标志、置顶 ID、重复项、下一游标及异常。
- `report.json`：目录遍历状态、选定话题范围、关联证据、原状态文件前后哈希和限制。

分页遵循 `messages.forumTopics.order_by_create_date`：为 true 时使用话题创建日期，
否则使用最后返回话题的 `top_message` 对应消息日期；同时传其最新消息 ID 和话题 ID。
保留服务端话题顺序（包括置顶），不按话题 ID 重排。缺失游标消息时按 ID 补查；
无安全游标、重复游标、排序模式改变或预算用尽则停止且保留审计，不伪报完成。
短页与 API count 都不是结束条件。两次空页结束、集合/名称相同且计数吻合也只代表
稳定观测，**不是严格服务器快照，更不是全部历史话题证明**。
每遍目录最多 1000 页、选定话题最多 100 页；失败重跑建立新采集目录，不冒称冻结快照恢复。

完整消息的论坛回复头优先 `reply_to_top_id`，其次论坛标志下的 `reply_to_msg_id`；
话题创建服务消息用自身 ID；确认是 forum 的完整普通消息无论坛回复头时属于 General（ID 1），
普通回复的顶层线程 ID 不当成话题 ID。空消息、缺字段或跨来源歧义保持未知并记录原因。
话题缺名、删除、ByID 未返回和查询失败分别记录，名称是采集时名称而非发布时历史名称。

**本轮不扫描全群视频。** 其他话题的 `observed_video_count=0` 仅表示本次未观测到，
不是没有视频；`accessible_video_total=null`。只有话题 6557 的实际分页范围另列于报告。
不把 `unread_count/top_message` 当视频数，不从“第2集 / 2.1.mp4”推断分段、缺集、全集或播放顺序。
`TOPIC_METADATA_VERIFIED` 仅指目录和单条视频话题关联验收，不表示全量下载完成。
该轮采集未修改原全量流程；当前保护策略见文首。话题目录自动接入原回填尚待单独实施。

### 本机真实元数据验收（2026-10-19）

输出目录：`/Users/qj/python_project/telegram-video-archive/archive/topics-20261019-184029-928673/`。
话题 6557 的采集时名称为 **告白母子：妈妈陪读也陪爱**，消息 6559 的论坛回复头、
ByID 查询、选定话题 GetReplies 结果及既有探测媒体身份一致。
选定话题返回消息 6557/6558/6559，其中 6557 是创建服务消息，6558/6559 是视频；
只有 6559 对应已下载且本次离线复核的文件，6558 未下载。不据此宣称全集齐全。

两遍目录均为 10 页（含末尾空页），每遍数量为
`101,100,100,100,100,100,100,100,92,0`，各 893 个唯一话题，集合和名称相同。
所有 893 条本次均有名称，包含 General，未观察到置顶话题。
请求 limit=100 但首页实际返回 101 条，API count 始终为 892；
**差异原因尚未确定**，不能仅因包含 General 就认定其解释了差值。
因此真实报告保留 `METADATA_COLLECTED_WITH_LIMITATIONS`，不写成严格快照完整。

离线验收命令（不会联网或重复下载；补充导出和审计不修改原采集时间/结果）：

```sh
/Users/qj/python_project/telegram-video-archive/.venv/bin/python /Users/qj/python_project/telegram-video-archive/validate_topic_metadata.py /Users/qj/python_project/telegram-video-archive/archive/topics-20261019-184029-928673
```

验收文件：上述目录的 `acceptance.json` 和 `selected-video-evidence.json`。
34 项测试通过（原 19 项 + 新增 15 项），pip check 通过，两个 SQLite 完整性为 ok；
导出凭据字段审计通过。原回填库、进度/总报告与原探测元数据前后哈希未变；
原回填仍 0 条消息/视频、`INCOMPLETE`。验收 `passed=true` 表示检查通过，
**不消除目录 count 差异，也不表示全量归档完成**。

分页与归属依据：
- https://core.telegram.org/api/forum
- https://core.telegram.org/method/messages.getForumTopics
- https://core.telegram.org/constructor/messages.forumTopics
- https://core.telegram.org/api/offsets
- https://github.com/telegramdesktop/tdesktop/blob/dev/Telegram/SourceFiles/data/data_forum.cpp

## 开发验证

```sh
cd /Users/qj/python_project/telegram-video-archive
/Users/qj/python_project/telegram-video-archive/.venv/bin/python -m unittest discover -s /Users/qj/python_project/telegram-video-archive/tests -v
/Users/qj/python_project/telegram-video-archive/.venv/bin/python -m pip check
```

文档核实依据（本次实现查询的稳定版为 1.45.0；Telethon v1 是持续更新 API 层/修复的维护版本）：

- https://docs.telethon.dev/en/stable/
- https://docs.telethon.dev/en/stable/basic/signing-in.html
- https://docs.telethon.dev/en/stable/modules/client.html
- https://core.telegram.org/method/messages.getHistory
- https://core.telegram.org/api/offsets
- https://pypi.org/project/Telethon/1.45.0/

本阶段停在本地下载验收，不创建远程仓库，不提交推送，不做 Supabase 或定时同步。
