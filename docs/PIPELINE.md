# v2 pipeline：实现、验收边界与操作

当前目录与脚本位置见 [项目结构](STRUCTURE.md)。代码按用途分组，下载队列按整剧推进；视频下载、上传、校验和缓存清理内核保持原实现。下文历史记录中的旧机器路径和运行事实不改写。

## 完整主流程与整剧处理（2026-10-28，当前入口）

默认 `run` 串联完整源扫描、整剧筛选、下载补齐验收、合格剧 AI 生成与事务回写、已有 faststart 待办和最终封存。`plan/batch/retry/repair/ai/transcode-backlog` 保留为分阶段入口。剧的完整成员以 **Telegram 原始话题在冻结边界内、当前 API 可访问的全部视频**为准，生产库、本地视频池和旧 `topics.json` 不决定剧清单。旧的数量与留存率需要重新扫描后计算。本轮没有执行真实源扫描、生产下载或部署；下文带日期的授权、运行状态和验收数量是历史记录。

### 默认 `run` 全链路

使用既有 Telegram 登录、来源白名单、数据库、存储和 AI 配置。以下命令均从仓库根目录执行，并使用已安装项目依赖的 Python 环境：

```sh
python pipeline.py run --drama-concurrency 2 --concurrency 4
```

`run` 默认完成以下步骤：

1. 完整源元数据扫描，只发现话题和成员，不下载媒体。
2. 按当前项目的已处理池排除已封存来源/话题，再执行整剧画质硬筛。
3. 合格剧进入剧通道，完成下载、上传、读回、有界补齐和最终下载验收。
4. 下载验收通过的剧生成 AI 标题、标签和简介，通过现有事务接口写回数据库；已经成功提交的 AI 任务直接复用。
5. AI 之后沿用现有逻辑处理历史 faststart 待办，完成后提交状态。新视频没有转码任务时按“不适用”处理，不新增转换任务；视频封面仍须完成或明确无缩略图。
6. AI、封面和实际存在的后处理任务全部完成后，将剧写入已处理池，之后默认跳过。没有转码任务不会让剧一直待定。

显式复用已有有效完整源扫描：

```sh
python pipeline.py run --skip-scan --drama-concurrency 2 --concurrency 4
```

`--skip-scan` 只省略本次扫描，不能绕过源完整性证据。没有有效完整扫描时仍停止。`plan` 仍是纯离线源质量预览，只读完整本地源快照，不读取已处理池、不查云端。手动扫描、分阶段下载、补齐、AI 和已有封装待办入口继续保留，例如：

```sh
python archive.py scan
python pipeline.py plan --minimum-quality 720 --report archive/drama-quality-plan.json
```

### 源完整性与整剧筛选前提

`archive.py scan` 是纯元数据入口：

1. 沿用首次任务冻结的消息 ID 上界和排他 UTC 时间边界；未建任务时建立边界。按原全局历史分页机制扫描到空页，中断后从已提交游标恢复。
2. 每次独立获取两遍源论坛的完整话题目录，核对话题集合及稳定计数（允许已知的 General 一条偏差）；不从已有 dramas/videos 反推话题。目录变化或计数异常时停止。排除 General 后，对每个源话题逐页调用 `GetReplies` 到空页，补充此前池子中没有的视频。
3. 核对来源、请求话题与实际归属、消息 ID 上界和时间边界，记录每话题全部视频的媒体身份、大小、时长、宽高及完成证据。全局扫描和全部源话题均通过，才标记源完整性成功；失败会清除本轮成功标记，旧证据不能顶替失败的新扫描。

扫描不下载视频或封面，不调用 `prepare_downloads`、下载器、FFmpeg、ffprobe 或媒体校验，也不重置已有视频下载状态。`scan` 的退出码 0 表示元数据阶段完成，不能解释为视频下载或验收完成；最终本地视频报告可以仍为 `INCOMPLETE`。

本地 `state.sqlite3` 的 `topic_inventories` 保存每话题完成证据，任务记录保存完整源话题 ID 集合及共同边界；`source-topics.json` 和 `source-inventory-*` 目录保留此次源目录及分页审计。`plan/batch/run/retry/repair` 要求这些证据全部有效，并据此构造完整成员快照。分页中断、空消息、来源/归属不明确、媒体身份变化或边界不一致时停止，不能退回旧池子继续筛选。元数据的宽高、时长或大小不足以计算画质时，该剧为 `pending`，等待补齐。

这里的“完整”指当前账号在冻结范围内可通过 API 枚举的内容。Telegram 没有历史原子快照，已删除、不可见或扫描期间发生变化的内容不能保证被取得。

### 画质规则：`drama_quality_v1`

`video_archive/screening/quality.py` 对完整剧成员统一评估，返回 `accept/reject/pending`，不按判断结果删除单集。判定顺序如下：

1. **画面方向。** 任一集 `width >= height`，整剧拒绝，包括方屏。竖屏只要求 `width < height`，不要求固定 9:16。已知横/方屏的拒绝优先于其他集的字段缺失；其余缺字段剧为 `pending`。
2. **稳定剧。** 宽、高各自的 `(最大值 - 最小值) / 最大值 <= 10%`，且最高/最低码率 `<= 1.5`，取各集短边的中位数与 720p 等效码率的中位数判档。
3. **差异较大的剧。** 先逐集判画质档。唯一主档占比 `>= 60%`，且低于主档的视频比例 `<= 10%`，采用该主档；否则使用最低分辨率档，并以该分辨率档内的等效码率中位数判档。比例按真实集数计算，1–9 集不能忽略 1 集，10 集才允许 1 集低质异常。

分辨率取 `min(width, height)`。以下门槛同时满足才达到对应画质档，取能达到的最高档；容差固定为 10%：

| 画质档 | 短边最低像素 | 720p 等效码率最低值 |
|---|---:|---:|
| 近 1080 | 972 | 1.5 Mbps |
| 近 720 | 648 | 1.2 Mbps |
| 近 480 | 432 | 0.8 Mbps |
| 低于 480 | 不满足以上任一档 | — |

默认最低要求是近 720 档，`--minimum-quality 480/720/1080` 可切换最低档。码率优先使用有效元数据；没有现成码率时按 `expected_size × 8 / duration_seconds` 估算平均 bps，再按 `bps × (720 × 1280) / (width × height) / 1,000,000` 换算等效 Mbps。已有已验证的本地探测结果可以补齐缺失声明字段，源声明字段优先。该值包含容器与音频等开销，是压缩程度的近似信号，不能视为视频流精确码率或感知画质评分；本轮不会为补字段预先下载文件。

报告中 `evaluated_dramas` 是参与评估的有视频源剧数，`quality_decisions` 分别列出合格、拒绝和待补字段的剧数，`accepted_drama_videos` 是所有合格剧的完整视频数。每剧记录保留判定原因、档位、方法、缺字段数及完整 `expected_message_ids/expected_video_ids`。`eligible_before_cloud_check` 则是经过原下载条件后本次能进入正常队列的视频数，可能低于合格剧的视频总数；`deferred` 说明文件大小、partial 等延后原因。`plan` 只读本地源快照，不读取已处理池、不查云端是否已归档，不下载或上传媒体。执行入口 `run/batch/retry/repair/ai/transcode-backlog` 会按池排除已处理剧。

### 剧通道、并发与计划

完成源扫描后，先用 `plan` 查看筛选结果。限制本次只处理一部剧：

```sh
python pipeline.py batch --drama-limit 1 --minimum-quality 720
```

- 每个剧通道从领取当前剧起，持续处理它的所有下载分段、失败补齐和最终下载判定，结束后才接下一部剧。不能先换剧，再回头给上一部补齐。
- `--drama-concurrency` 默认 1，可开多个剧通道。`--concurrency` 默认 4，是全部剧通道共享的视频任务上限；两者不相乘。例如两个剧通道、视频并发 4，合计仍最多同时处理 4 个视频任务。
- `--batch-size` 默认 100，最多 500。大剧可以跨多个分段，分段不改变剧通道的占用范围。
- `--only` 和 `--first` 中任一消息 ID 都会扩展到所属整剧，质量判断和最终判定始终使用完整源成员。
- `--limit` 是本次视频数量软目标：当前剧完整纳入后，达到目标才停止接新剧，不截前几集。`--drama-limit N` 则直接限制本次剧数。

需要两个剧通道时，仍共享全局四个视频任务名额：

```sh
python pipeline.py run --drama-concurrency 2 --concurrency 4
```

文件传输、媒体身份检查、上传、读回校验、租约和缓存清理沿用原实现。`verified` 表示当前流水线已完成下载验证、上传读回及云端提交；不要求整部剧所有文件同时保存在本地缓存。视频完成与封面完成分别记录。

### 有界补齐与最终缺失率

每个剧通道先做本轮初次处理，再默认最多补齐 4 轮。数据库累计最多 5 次尝试、有效租约和退避继续生效；遇到租约占用、次数耗尽或硬性大小约束，记录原因，不强行重置。单集失败和每次补齐历史都保留。

最终缺失率按完整源清单计算，按唯一视频身份去重：

`缺失率 = 尚未 verified 的预期唯一视频数 / 源清单全部预期唯一视频数`

| 最终情况 | 整剧结果 | 后续 AI/转码 |
|---|---|---|
| 全部预期视频已 verified | `complete` | 可以进入 |
| 补齐结束后仍有缺失，缺失率严格小于 10% | `accepted_with_gaps` | 可以进入，保留缺失清单 |
| 补齐结束后缺失率大于或等于 10% | 拒绝 | 不进入 |
| 硬停止或中断，尚未完成最终判定 | `pending` | 不进入 |

例如源清单 50 个视频，缺 4 个为 8%，补齐结束后可通过；缺 5 个刚好为 10%，拒绝。不能在发生硬停止或中断时，提前用当前缺失率放行。这里的下载缺失率和画质评分中的“低质异常最多占 10%”是两个独立规则：前者衡量视频是否完成验证提交，后者只影响下载前的画质评分。

### `repair`：审计旧数据并补缺

```sh
python pipeline.py repair
```

`repair` 不需要旧的 `--report`。它按当前完整源清单与已验证的云端提交重算每部剧的缺口，保留合格剧完整成员，只为缺失视频安排处理；已有 partial 沿用续传。已经全部 verified 的剧只做审计，不重新下载。源信息不完整或画质不合格的剧仍不能绕过前面的关口。

`repair` 与下载共用剧通道、最多 4 轮补齐和最终缺失率判定；达到租约、尝试次数或硬限制的条目保留原因，不能靠 repair 清空计数再来。旧报告仍可通过 `retry --report ...` 恢复，但不是修复缺口的前提。

### 持久判定与后续处理门槛

完整源 manifest、逐视频失败、补齐历史和每部剧的最终判定持续保留。整剧结果注册表位于：

`archive/pipeline/projects/<project_ref>/drama-outcomes.json`

`ai`、`transcode-backlog` 及归档后的自动转码只接收已判定通过（`complete` 或 `accepted_with_gaps`）且与当前完整 manifest 一致的剧。拒绝、`pending` 或从未评定的旧数据不进入；旧数据可先运行 `repair`，全齐时无需重下载即可完成审计。后续只处理已经完成云端提交的视频，缺失成员继续留在清单中。

拒绝或待定不会物理删除已经上传的云端对象，本地缓存仍按原验证与清理条件处理。`batch/run/retry/repair` 每次先验证源完整性和画质条件；旧 `archive.py sample/backfill/resume/retry` 保留本地归档用途，不套用新筛选、剧通道或最终缺失率判定。

### 已处理池、跳过和手动解除

已处理池与下载验收结果分开保存，并按目标项目隔离：

`archive/pipeline/projects/<project_ref>/processed-dramas.json`

| 池内状态 | 入池条件 |
|---|---|
| `screening_rejected` | 整剧画质条件已明确拒绝 |
| `download_rejected` | 下载补齐已结束，整剧被最终拒绝 |
| `completed` | 下载验收通过、AI 成功事务提交、已下载视频封面完成或无缩略图，且实际存在的后处理任务全部完成；无转码任务为不适用 |

`complete` / `accepted_with_gaps` 是下载验收结论，不能单独视为完整流程的 `completed`。只有所有后续阶段也完成，才封存为 `completed`。

临时断网、信息不足、预算或大小限制暂缓、AI 失败、封面或已有 faststart 任务待完成不入池，下次沿用已有进度继续。已成功提交的 AI 任务不会在重跑时重新付费生成。源目录扫描仍做完整发现；执行入口在后续画质评估或媒体处理之前按池排除已处理剧，纯离线 `plan` 不受池影响。

池子按来源和原始话题识别剧，不因话题新增视频自动失效：一旦入池，同一话题后续新增视频也默认跳过。需要再次处理时手动解除：

```sh
python pipeline.py processed
python pipeline.py reopen --drama-id UUID
# 一次解除多部剧
python pipeline.py reopen --drama-id UUID_1 UUID_2
```

将占位符换成 `processed` 显示的实际剧 UUID。`reopen` 只解除跳过，不清空数据库尝试次数，不重置已成功的 AI 任务，也不删除已上传对象。解除后再运行正常流程，会重新按当前源清单和现有进度判断。

本轮只完成实现与离线测试，没有执行真实 Telegram 扫描、生产下载或付费 AI 调用。实际源清单、合格数量和全流程结果需在配置正确后执行对应命令取得。

## main 授权与恢复入口历史记录（2026-10-21）

当前实际状态（2026-10-21 16:31）：main表/角色/私有桶已建成；唯一一次启动PID65100已在领取前因urllib_URLError退出，当前无worker、锁空闲。刚补只读GET/HEAD最多3次请求建立重试以及权限门的有界连接重试，尚未在main重启验证。不要重跑初始化。最新事实和绝对报告路径见 `/Users/qj/python_project/telegram-video-archive/PROGRESS_HANDOFF.md` 顶部。

目标为 main `wbtsfzozlmurljvglhpn`，套餐 Pro（用户确认）、区域 ap-southeast-1（Supabase 项目详情核实）。用户授权在目标和基本权限核验后同轮初始化并启动，不再要求新增 test 样本验收。Dashboard 全局限制为用户确认的 500MB，不是 5GB；本机和私有桶采用保守的 500000000 字节有效上限，实际全局字节数没有通过设置 API 读回。

受限 worker 配置：`/Users/qj/python_project/telegram-video-archive/.private/main-pipeline.json`。owner 初始化配置单独保存在 `/Users/qj/python_project/telegram-video-archive/.private/owner-main-pipeline.json`，不用于长期 worker。

```sh
python pipeline.py --config .private/main-pipeline.json run --batch-size 100 --concurrency 4 --max-video-mib 128 --download-timeout 1800 --cache-high-gb 10 --retry-rounds 1 --cleanup
```

- 实际在途生命周期至多4个，包含元数据导入和状态查询；普通连接4个名额，续租独立2个名额。下载领取/结束不阻塞异步事件循环，下载每次进度检查也检查租约。租约120秒/每40秒续租保持不变；仍无法保证所有网络续租都成功。
- 正常队列先向后推进，之后等待数据库30秒退避并按本轮失败清单最多重试一轮（分段100，默认并发4），不反复处理队首；数据库最多5次领取和有效租约保护保持不变。最终失败/退避/尝试耗尽条目保留在报告中，不强行重置。
- `retry --report` 从同一项目的持久化报告恢复失败、未开始或过期运行候选，拒绝不同项目报告。`--report` 必须给真实报告绝对路径。默认仍按128MiB限制，deferred不会隐式加入；单独处理deferred时需要显式 `--include-deferred` 和恰当阈值（仍不得超过桶上限）。同时只允许一个worker持有项目锁。
- 续传通过Telegram `iter_download` 从offset恢复；已有 `.part` 复制为 `.part.resume` 工作副本，校验原前缀一致后追加，原 `.part` 保留不变。中断后继续工作副本；成功后先本地完整校验，再进入TUS/完整读回/事务提交/清理。主正常队列仍将所有既有partial和3021列为deferred。
- 队列报告含完整deferred清单、未开始ID及当前分段报告路径；分段每15秒刷新阶段字节/耗时。正常队列完成不是全5212条完成。
- Storage 401/403、资源冲突、持续数据库不可用、磁盘/缓存底线或长限流停止新领取；单文件失败保留记录并继续。短暂数据库连接错误在新健康查询成功时不停止整个队列。
- 原始资料、校验记录、3021和其他旧part以及6558/6559保护原件继续保留；test对象和成功标记不迁移到main。无真实AI、常驻或增量服务。

## 入口与边界历史记录（2026-10-20）

- 用户已确认样本门槛为 6558、6559。旧 SQLite 的 `sample_passed=false` 保留，禁止用旧 `archive.py backfill/resume` 启动云端全量。
- 新 `batch` 明确限制最多 100 个；新 `run` 在同一进程、同一项目锁和 Telegram 连接下分段处理有限清单，不是常驻服务。当前仅授权 test 的六条验收，main 全量仍需明确确认。
- 缓存按目标 `project_ref` 分目录保存在 `/Users/qj/python_project/telegram-video-archive/archive/pipeline/projects/{project_ref}/`。旧 `/Users/qj/python_project/telegram-video-archive/archive/pipeline/cache.sqlite3` 原样保留；其 tombstone 和上传会话不用于新项目。流水线每次以目标数据库为准；同一份本地原件可以复用。
- 默认只选不超过 128 MiB 的视频。更大的文件、3021、已有 `.part` 单独列为 deferred，不能将正常队列完成写成 5,212 个全部完成。阈值可显式调整，且任何超过目标桶实际限制的文件都不会进入下载。
- 每次运行每视频至多一次媒体传输，单文件默认超时 1800 秒，可用 `--download-timeout` 改为 30..86400 秒；跨次运行仍受数据库最多五次领取和退避限制。失败不在本轮反复重试；后续分段继续。已有部分文件不删除、不自动从头重下，等待单独恢复。
- 长时间 FloodWait、缓存持续不足或数据库连接重试耗尽停止后续领取；短 FloodWait 对新下载统一等待并降低并发。SIGTERM 协作停止，并等待上传线程结束后才释放项目锁。
- 报告每 15 秒刷新，含 Telegram 传输、下载/上传前校验、TUS PATCH 和完整远端读回耗时及字节数。`run` 同时保留每段独立报告，重启以目标库已提交对象为准。
- main 准备时由用户把全局、私有桶及本机配置的单文件限制统一为 5 GB；目前不自动修改已有桶。最大清单文件 4,033,019,179 字节。
- 本机 55440 隔离 PostgreSQL 下 94 项测试通过，依赖检查通过。本轮 test 实际验收结果见 `/Users/qj/python_project/telegram-video-archive/IMPLEMENTATION_RESULTS.md` 的最新记录。

```sh
# 离线查看普通/大文件分流，不联网、不代表目标库尚未归档数量
python pipeline.py plan --max-video-mib 128
# 本轮授权的 test 精确验收集合；目标配置必须仍是 test
python pipeline.py batch --limit 6 --only 10 20 21 25 26 7 --concurrency 4 --cleanup
# main 准备并取得启动确认后，才可执行：普通文件有限全量，不处理 deferred
python pipeline.py --config .private/main-pipeline.json run --batch-size 100 --concurrency 4 --max-video-mib 128 --download-timeout 1800 --cleanup
```

`--only` 精确限定集合，也允许指定已归档视频做读回复验。`batch` 必须有 `--limit`；`run` 不带 `--limit` 会处理本轮全部合格普通视频，有 `--limit` 则限制本轮总数。报告中 deferred/failed/未开始任务都必须保留待办；`normal_queue_archived` 不是全清单完成。AI 可后置，不阻塞原视频归档。无需新增业务表或字段。

## 早期两样本阶段记录（历史，不代表当前状态）

- 云端业务表严格只有 `drama_archive.dramas/videos/ai_results/pipeline_tasks`，字段按 v2。Storage 系统表不迁移到业务 schema。
- 已实现 SQL 迁移、受限事务函数、稳定 UUID、只读旧库导入、三阶段领取/续租/过期回收/退避/条件提交、TUS 分块恢复、读回 SHA-256、受保护缓存、AI 接口和逐字段回退。
- 本机 PostgreSQL 14 运行真实 SQL/并发/权限测试；TUS 是协议替身测试，不是 Supabase 服务实测。AI 是显式测试替身，不是模型验收。
- 本机两个原件重新执行大小、SHA-256、ffprobe、完整 ffmpeg 解码通过。报告：`/Users/qj/python_project/telegram-video-archive/archive/pipeline-local-acceptance.json`。
- 没有 Supabase 配置，尚未核实远端项目、区域、套餐及实际全局限制；**没有创建云端资源、上传对象或调用模型**。
- 6558 为 54,712,104 字节，6559 为 56,592,060 字节。两者均超过 Free 50 MiB；不得自动升级套餐。一次上传+一次完整读回至少涉及 111,304,164 字节上传和同等读回；重试、重复验收还会增加读流量，价格必须以目标项目结算区域和套餐确认。
- 只读清单：5,212 视频可关联 891 个有视频话题；893 目录项并不都含视频。General 在导入前排除。
- 3021 仍 pending，原因为 `interrupted_file_will_restart`；原样本集 `[3021,6559,6558]`、`sample_passed=false` 不变。后续需补完 3021，或由用户明确变更样本集，不能用两样本云端验收替代三样本门槛。
- 无 Git 仓库；原修改文件备份在 `/Users/qj/python_project/telegram-video-archive/.private/implementation-backup/`。原 SQLite schema 未改；缓存采用独立 SQLite。

## 安全配置（只在本机填写）

新增依赖 `psycopg==3.2.10`、`psycopg-binary==3.2.10`、`typing_extensions==4.16.0` 已锁定；用途为 PostgreSQL 事务访问。HTTP/TUS 使用标准库，不依赖 Supabase SDK。

```sh
python -m pip install -r requirements.lock
install -m 600 config/examples/pipeline.config.example.json .private/pipeline.json
chmod 700 .private
```

**只在首次配置时执行 install，避免覆盖已有安全配置。** 用本机编辑器打开 `/Users/qj/python_project/telegram-video-archive/.private/pipeline.json`，不要把内容贴进聊天、终端参数或报告。

必要字段：

| 字段 | 来源和要求 |
|---|---|
| project_ref | Dashboard 确认目标项目的 20 位 ref |
| region / plan | Dashboard 人工核实的部署区域与套餐；程序不冒称此值已远端验证 |
| database_dsn | 直连 `db.<project_ref>.supabase.co` 的 PostgreSQL DSN，数据库名通常 postgres，必须 TLS；建议 sslmode=verify-full，配置受信 CA；当前不接受 pooler/自定义域名 |
| storage_key | 本项目服务端 Storage secret key 或 legacy service_role JWT；不使用普通客户端 key，不打印 |
| global_file_limit | Dashboard Storage settings 实际全局限制（字节）；不是仅根据套餐猜测 |
| bucket_file_limit | 用户同意的桶单文件上限（字节），不能大于全局限制 |
| sample_budget_confirmed | 确认目标、两样本存储/上传/读回预算后，本机手动设 true；否则拒绝网络操作 |

没有自动读取其他项目凭据、Management API token 或 Telegram session。区域/套餐/全局限制目前依靠管理员 Dashboard 核实，不具备自动 Management API 验证。`check` 会验证私有桶的实时配置，但不能证明 Dashboard 元数据正确。

## 权限与迁移

首版使用 **PostgreSQL 直连，而非 PostgREST**。无需把 `drama_archive` 加入 Data API Exposed schemas，应保持未暴露。普通客户端通过未来可信后端取得展示字段；此轮不提供公网播放 API，也不存签名 URL。

- 四表启用 RLS，匿名/authenticated/service_role 没有 schema/table/function 授权。
- `drama_worker` 为 NOLOGIN、NOBYPASSRLS 组角色，只拥有 schema USAGE 和指定 SECURITY DEFINER 函数 EXECUTE，无直接表读写。
- 数据库管理员在本机安全数据库控制台创建专用 LOGIN，授予 `drama_worker`，通过交互式密码设置或安全控制台设置密码，运行时 DSN 改为此 LOGIN。不能把数据库 owner 用作长期 worker。
- 迁移使用 owner DSN；先确认目标/预算，再执行下列命令。已有同名 schema 会明确停止，不 DROP、不自动“兼容”覆盖；需要管理员审查。重复执行数据导入是幂等的，DDL 不是盲目重放。
- Storage 服务密钥有较大权限，必须留在本机私有配置；代码固定桶和对象键、禁止 upsert 和跨主机重定向。普通客户端不拿此 key。桶通过 Storage API 创建，不能直接插入 storage 系统表。

```sh
python pipeline.py migrate
# 在本机安全控制台创建 worker LOGIN、GRANT drama_worker TO 该LOGIN，改为受限 DSN
python pipeline.py bucket
python pipeline.py check
```

`drama-videos` 必须私有、文件限制与配置一致；`drama-covers` 必须公开、限制 5MiB（见下节）。同名但限制/公开性/MIME 策略不符的桶一律停止，不修改已有资源。迁移权限已在本机 PostgreSQL 验证，Supabase 默认权限和目标实例兼容性仍需远端测试。

## 封面：Telegram 缩略图（2026-10-22 新增）

每个视频额外归档一张 Telegram 自带的最大缩略图作为封面。封面与视频是**两条独立的任务线**：任一方失败、退避或尝试耗尽都不影响另一方，各自有 `pipeline_tasks` 阶段、各自计 attempts 和租约。全程不经过 ffmpeg/ffprobe，封面只做「非空 + JPEG magic + sha256」校验。

- 阶段：`cover_download`（取 `doc.thumbs` 中最大的 `PhotoSize`/`PhotoSizeProgressive`，存 `archive/covers/<视频同名>.jpg`）→ `cover_upload`（单次 POST 进公开桶，读回比对 sha256，提交 `videos.cover_path`）。`cover_download` 不依赖视频的 download/upload，`cover_upload` 只依赖 `cover_download`。
- 唯一对应关系只靠同一个 video uuid：本地 `covers/<源ID>_<消息ID>_<媒体ID>_<原名>.jpg`、对象 `drama-covers/covers/<uuid>/original.jpg`、数据库 `videos.cover_path`（CHECK 绑定 id）。不存在额外映射表。
- 公开桶是为了封面的首屏速度：省掉每张图的签名往返，并能被 CDN 与浏览器缓存（`Cache-Control: max-age=31536000, immutable`）。**对象键可由源ID和消息ID反推**，等于「知道频道和消息号即可看封面」。视频桶保持私有，不适用此结论。
- Telegram 本身没有缩略图时记 `videos.cover_note='no_thumbnail_available'`，`cover_upload` 同时关闭。缺封面的查询口径是 `cover_path IS NULL`，原因看 `cover_note`，并有部分索引 `videos_missing_cover` 支撑。ffmpeg 抽帧兜底不在本轮范围内；真要补，先由 owner `UPDATE drama_archive.videos SET cover_note=NULL` 放开该行再跑。
- 已归档但封面未完成的视频仍会被下一轮正常队列选中，其视频线直接记 `video_already_committed` 返回，不会为了取封面重新读回整个视频对象。封面并发独立且较低（默认 2），因为封面线会自己 `get_messages` 刷新消息，这是视频线没有的额外 RPC。
- 报告新增 `cover_counts`/`peak_covers`，每条记录带 `cover_result`/`cover_error`/`cover_seconds`；`index.csv` 与 `messages.jsonl` 新增 `cover_status`/`cover_path`/`cover_reason`。封面状态**不参与**视频完成门槛和 `report.json` 的 `result` 判定。

在已有 schema 上增量迁移（只接受显式指定的单个文件，不重放初始迁移；owner DSN）：

```sh
python pipeline.py --config <owner配置> migrate --migration 004_cover.sql
python pipeline.py bucket   # 建/校验两个桶，受限 DSN
```

`migrate --migration` 要求 schema 已存在，`migrate` 不带该参数时要求 schema 不存在，两者互斥。004 只改列、stage 取值域和 `import_video`/`claim`/`finish` 三个函数体，表集合与函数集合不变，`verify_worker()` 的契约断言照常通过。已导入的旧视频在下一次 `import_video`（每轮每条都会调用）时自动补上两行封面任务。

## 仅清单和两样本命令

```sh
# 离线，只读现有 SQLite 和指定话题目录，不访问 Telegram
python pipeline.py inventory
# 离线重验两个原件，单独报告，不改原 report/progress/sample_passed
python pipeline.py verify-samples
# 可选：仅导入已扫描元数据，每视频三条 pending；不下载/上传/调用 AI
python pipeline.py import
# 只导入/归档 6558、6559，保持 AI pending，完成即停止
python pipeline.py samples
```

启动会取得现有 `/Users/qj/python_project/telegram-video-archive/.private/process.lock`，与原下载互斥。Ctrl-C 或向该进程发送 SIGTERM 停止；再次执行同一个 `samples` 命令恢复。失败任务默认退避 30 秒、最多 5 次领取、租约 120 秒，每 40 秒续租；SIGKILL 后等待租约过期。耗尽重试后不自动无限循环，由管理员检查原因后受控重置任务（不要删除视频重建 UUID）。不启动任何批量常驻 worker。

稳定 UUIDv5 由来源 ID 与话题/消息 ID 派生；媒体 ID 不允许变更，跨来源复合 FK 防错配。消息原文不会导入云端。导入冲突事务回滚，不更新原字段掩盖变化。

## 恢复、远端校验及缓存

- 上传对象键固定 `videos/{video_id}/original`，每块至多 6 MiB，文件不整体读入内存。
- 上传前先尝试完整读回既有对象：相同 SHA-256 补交事务，内容不同停止，绝不覆盖。新上传后再次完整读回计算 SHA-256；ETag、大小、客户端 metadata 都不当作摘要证明。
- TUS URL 仅存本机权限受限 SQLite，HEAD 取实际 offset；404/410 的过期会话重建，不盲信本机 offset。网络失败退出保留文件/会话，下次恢复。完成 PATCH 但响应丢失也由读回核对恢复。
- 对象无法由 DB 事务回滚。对象成功、数据库失败时保留文件，后续先核对对象再提交。对象路径与 upload succeeded 同事务，AI 输出与 ai succeeded 同事务；过期 token 不能提交，即使尚未有新 worker。
- 缓存默认高 20 GiB、低 12 GiB、系统底线 5 GiB；实际文件及并发预留纳入预算（增长期间保守双计，宁暂停不超配）。预留和暂停位由 SQLite 事务保护；崩溃预留只在 OS 文件锁已释放时回收，不能仅按 TTL 猜测 worker 已死。
- 清理前要求真实读回摘要、本地摘要一致、数据库归档成功、AI 待办存在、没有读取者。先写 tombstone 再删除，崩溃最多遗留副本。AI 输入读取与清理用同一个 OS 锁。
- 两个真实样本永久标记 protected，本轮无 CLI 清理入口。清理测试只用临时文件。未完成的 3021 不删除。
- 新 pipeline 恢复先检查云端 upload 状态，本地缺失不会触发 Telegram。原 archive.py 增加本机云端 tombstone 跳过保护；**若本机缓存数据库被丢失，禁止直接运行旧 resume，先用新 pipeline 对照云端恢复本机记录**。原本地完整验收报告仍保持本地语义，不把缺失文件伪报为本次解码成功。
- 下载阶段保留旧来源白名单、媒体 ID/大小、General/TTL、完整性验证及退出清理。`download_task` 提供有界缓存接入，但本轮没有开放全量调度 CLI；旧 archive.py 全量命令不是新的有界云端流水线，不应拿它启动 608 GB 全量。

## AI 独立接口（2026-10-21 新增 `ai` CLI 命令，已接入 OpenAI 兼容多模态接口）

`run_ai` 只处理 upload 成功任务。优先读取有锁的本地缓存，否则按本地大小/SHA 记录从私有桶读回受限临时文件；缺少完整性记录时停止，不从 Telegram 重下。提供商需实现 `analyze(path, check=...)`，定期调用 check 检查租约。输出严格为 title/tags/description；字段允许未知，不要求编造。

成功覆盖单行，失败保留旧结果。原始字段分别优先，trim 空白、标签去空去重、两套标签不合并。测试替身默认拒绝，而且只允许显式本机 `drama_pipeline_test` 数据库启用；无生产 AI CLI。

`pipeline.py ai [--limit N]` 已接线：候选集合 = 本地清单中 message_id 落在云端 `archived_messages()`（即 upload 已提交）的视频，不新增 SQL 函数。每个候选直接尝试 `run_ai`；`claim()` 已有的条件（ai 阶段 pending/失败退避/过期租约、attempts 上限）决定是否真正执行，不可领取时该条记为 `skipped`，单条失败记为 `failed` 并继续下一条，不做常驻调度/批量重试。

- `pipeline_ai.py`：`extract_frames()` 用 ffmpeg 按时长均匀抽帧（默认 6 帧、长边 768px，避开首尾 5% 的片头/片尾）。`call_model()` 是唯一的网络调用入口：POST `base_url + '/chat/completions'`（OpenAI chat completions 兼容格式），帧以 `data:image/jpeg;base64,...` 形式随系统提示词一起发送，系统提示词取自 `prompts/prompt.md`（非密钥，明文文件）。响应内容按 ```` ```json ```` 围栏做宽松解析后 `json.loads`，再交给 `ai_output()` 做唯一的严格字段校验。任何 HTTP/JSON 异常都走现有 `error_code()`，不记录密钥或响应正文。
- 密钥与模型参数在独立文件 `.private/ai.json`（0600，与 `.private/pipeline.json` 权限模型一致，示例见 `config/examples/pipeline.ai.config.example.json`），由 `pipeline_common.ai_config()` 校验：`provider`/`model`/`api_key`/`base_url` 必填，`base_url` 必须是 `https://`；`frame_count`(1-20，默认6)、`max_dim`(64-2048，默认768)、`request_timeout`(10-600秒，默认120) 有范围检查。
- 当前配置的 provider 是一个 OpenAI 兼容网关（`api.surplusintelligence.ai`），模型 `gemini-3.5-flash-lite`；这是用户提供的第三方代理服务，不是 Anthropic/OpenAI/Google 官方直连，费用和可用性以该服务为准，程序不做额外验证。

## 测试

普通离线回归（PostgreSQL 测试会明确 skip）：

```sh
python -m unittest discover -s tests -v
python -m pip check
```

本轮 PostgreSQL 测试连接本机临时实例 127.0.0.1:55439，库名强制 `drama_pipeline_test`；测试会清空**这个测试库的 drama_archive schema**，拒绝远端/其他库名，不要改成生产连接：

```sh
DRAMA_TEST_DSN='host=127.0.0.1 port=55439 user=postgres dbname=drama_pipeline_test' python -m unittest discover -s tests -v
```

最终实际测试次数与结果见 `/Users/qj/python_project/telegram-video-archive/IMPLEMENTATION_RESULTS.md`。尚未覆盖的真实环境条件：Supabase TUS/Storage 密钥版本兼容、实际远端 RLS/角色、目标区域套餐限制与费用、真实对象冲突/网络崩溃、真实模型。离线替身通过不消除这些验收项。
