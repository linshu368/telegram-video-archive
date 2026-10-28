# Telegram 短剧归档交接（2026-10-21 更新）

## 最新交接：main 正常队列已真正启动并在跑（2026-10-21 16:54 北京时间，覆盖下文"16:31"快照）

本节覆盖并取代紧邻其下的"16:31 已初始化但全量未真正运行"快照；那一条此前两次启动均在领取前失败（PID 65100 urllib_URLError、随后一次 psycopg_OperationalError），本轮是第三次尝试，已确认真正进入下载/上传。

### 启动前有界诊断

- 启动前 `check`（只读）一次通过：restricted_login/four_tables_rls=true、direct_table_access/client_access=false、bucket private=true、file_size_limit=500000000。
- 离线 `plan --max-video-mib 128` 核对：`eligible_before_cloud_check=3720`、`bytes=214266407115`、`deferred_count=1492`（本次 Counter 口径为 `large_video=1487`+`explicitly_deferred=1`+`existing_partial_preserved=4`，与旧口径 large_video1376+bucket_file_limit111 之和一致，不是数字变化）。
- 启动前 `ps`/`.private/process.lock`（0 字节）均确认无 worker、锁空闲；磁盘可用约 50GiB。
- 第一次本轮 `run` 启动（16:43）仍在领取前因 `psycopg_OperationalError` 退出，未生成 `pipeline-run-*` 报告；随即连续 3 次 `check` 全部成功，判定为已知间歇性网络抖动（文档历史上同时记录过 urllib_URLError 和 psycopg_OperationalError 的间歇性失败），不是持续性整体连接不可用，因此做了一次有界重试，未重放迁移/初始化。

### 本轮真实启动（第二次尝试，16:48:51 发起）

- 命令：`pipeline.py --config .private/main-pipeline.json run --batch-size 100 --concurrency 4 --max-video-mib 128 --download-timeout 1800 --cache-high-gb 10 --retry-rounds 1 --cleanup`，nohup 后台运行，已 disown，不受本次会话终止影响。
- 实际 PID 67355；16:54 核查仍存活（已运行约 4 分 14 秒，12.3% CPU，RSS 约 97MB）。
- 启动器 stdout/stderr 日志：`/Users/qj/python_project/telegram-video-archive/archive/main-run-stdout-20261021T084851-retry1.log`（pipeline 自身输出走 report 文件，这个日志目前为空属正常，只有异常退出才会有 STOP 行）。
- 主报告：`/Users/qj/python_project/telegram-video-archive/archive/pipeline-run-2026-10-21T08-49-14.619237+00-00.json`（status=in_progress，requested=3720，deferred=1492，ai_called=false）。
- 当前段报告：`/Users/qj/python_project/telegram-video-archive/archive/pipeline-run-2026-10-21T08-49-14.619237+00-00-segment-1.json`（第一段 requested=100）。16:54 快照：`counts={'failed':2,'uploading_or_verifying':1,'downloading':3,'queued':94}`，`peak_downloads=4`、`peak_uploads=2`，`flood_events=0`，`stop_reason=None`。失败 2 条为单文件失败且已继续推进，不是整段/整体失败：message_id 7（http_RemoteDisconnected）、message_id 10（urllib_URLError）。
- 16:54 缓存目录 `archive/videos` 约 427MB；磁盘剩余约 50GiB，未明显下降到警戒线。
- 第一次失败尝试（16:43）没有留下 `pipeline-run-*` 报告，只有一行 `STOP: psycopg_OperationalError` 的启动器日志；不要把它和本次真正运行的报告混淆。

### 新窗口/后续核查必须

- 这是正常队列的第一段（100/3720），远未完成；不能把本段或某个时间点的快照当作正常队列全部完成，更不能当作全部归档（仍有 1492 deferred 从未计入本次 run）。
- 下一轮先用 `ps -p 67355`、`.private/process.lock`、以及主报告/最新 segment 报告核实真实进度，不凭本节数字假设仍在同一进度；若进程已退出，读取主报告的最终 `status`（如 `all_archived`/`in_progress` 被中断/其他）和 `stop_reason` 判断是正常完成、单段结束等待下一段，还是系统性停止。
- 吞吐目前仍然偏低（历史已记录的已知问题，未在本轮解决，不是本轮新增阻塞项）：约 427MB/254s（进程 etime）左右，按此速度 214GB 正常队列预计数十小时量级；不要因为"启动成功"就推断速度问题已解决。
- 单文件失败（7、10）已按设计持久化并继续，之后应按失败清单做有界重试，不在本轮 run 内反复重试同一条。
- 不要以本节为由重新执行 `migrate`/`.private/initialize-main.py`/旧 `archive.py` 全量命令；worker 仍是受限登录，owner 配置本轮未使用。

## 最新交接（已被上一节取代）：main 已初始化，但全量未真正运行（2026-10-21 16:31 北京时间）

本节覆盖下文所有旧快照和旧门槛。用户要求本窗口只总结、更新交接并提供新窗口提示词；不要在本窗口再次启动。

### 实时核实

- main ref `wbtsfzozlmurljvglhpn`，项目 ST_telegrambot；区域 ap-southeast-1 已由 Supabase 项目详情核实；Pro 为用户确认。owner 实际连接核对 current_database=postgres、cluster_name=main，客户端TLS为true。pooler后端的pg_stat_ssl=false不代表客户端TLS未开启。
- 001、002、003已实际在main应用；四张业务表、8个受限函数和drama_worker/drama_worker_login已存在，禁止重放初始化或DROP。受限登录曾实际连接成功，并通过完整verify_worker审计（four_tables_rls=true、direct_table_access=false、client_access=false）。
- 私有桶 drama-videos 已实际创建，GET核验 private=true、file_size_limit=500000000。用户Dashboard确认全局为500MB，不是5GB；500000000为本机/桶的保守有效上限，未通过设置API读回全局精确字节值。
- 唯一一次main启动发起于16:22，记录PID65100；现已退出。16:31本机ps确认该PID不存在、项目锁idle，日志为STOP: urllib_URLError。没有main pipeline分段报告，启动在领取前结束；不能把launch_requested当作运行或归档成功。
- 启动前数据库已核实提交0条；本次启动未进入文件队列，未新增归档。16:31没有重新远端查询计数，不冒称新的实时云端计数。无本轮单文件失败报告；这次是启动失败，不是3720个文件都失败。
- main规划正常队列3720项，214266407115字节；deferred1492项：既有partial4、大于128MiB但未超过桶上限1376、超过桶上限111、3021明确后置1。合计5212。全部仍待推进，不能将正常队列完成当作全归档完成。
- 缓存281564915字节；16:31磁盘可用54984187904字节（约51.2GiB）。6558/6559原件存在且重新计算SHA-256与记录相符。旧part全部保留：20=4325376、21=4456448、25=4456448、26=4587520、3021=110100480字节。

### 凭据和证据路径（不得打印内容）

- 受限worker：`/Users/qj/python_project/telegram-video-archive/.private/main-pipeline.json`。
- owner仅初始化/只读核查：`/Users/qj/python_project/telegram-video-archive/.private/owner-main-pipeline.json`。
- 用户输入的main Storage/DB配置已分别私密保存，勿从其他项目补凭据。test原配置未覆盖，test对象不迁移、成功标记不复用。
- 初始化报告：`/Users/qj/python_project/telegram-video-archive/archive/main-initialization-20261021.json`，status=resources_initialized。
- 启动规划：`/Users/qj/python_project/telegram-video-archive/archive/main-launch-20261021.json`，旧status=launch_requested仅表示曾发起；worker_actual_status已补充为exited_before_intake。
- 日志：`/Users/qj/python_project/telegram-video-archive/archive/main-worker-2026-10-21T08-22-19.690117+00-00.log`。
- 最新本机核查：`/Users/qj/python_project/telegram-video-archive/archive/main-handoff-status-20261021.json`。

### 本轮代码和测试

- 已补报告retry入口、正常队列后有界重试分段、.part.resume独立副本续传（保留原part）、在途生命周期限额、异步领取/结束、独立续租连接名额、下载进度租约检查、Storage权限错误暂停。
- 初次worker启动失败后，补了两个启动抗瞬断改动：Database.verify_worker复用四次有界连接重试；Storage GET/HEAD建立请求最多三次重试（1/2秒退避）。POST/PATCH不自动重发。最后这个只读请求重试补丁尚未在main重启验证；不要描述为真实问题已经解决。
- 本轮完整本机101项通过（16.071秒，最后两处窄修改前）；随后13项调度、1项PG下载、3项PG权限门、6项Storage测试分别通过。没有重跑完整102项，不要累加/重复统计成新总数。
- 测试只允许已核实的127.0.0.1:55440/drama_pipeline_test（data_directory为本项目.private/pipeline-test-postgres），严禁将测试DSN指向Supabase。
- 真实网络连接仍间歇失败（psycopg_OperationalError和urllib_URLError），也多次连接/核验成功。尚无足够证据确认DNS、TLS、代理或上游哪一项是根因，不要猜成权限缺失或重建资源。

### 新窗口应继续

先读取本交接、v2设计、实施结果和PIPELINE，再实时核对单worker/资源兼容/权限。直接使用已有受限main配置恢复；不要运行`.private/initialize-main.py`重建。现有授权允许同轮启动run正常队列，并发4、分段100、128MiB、缓存高10GiB、cleanup、retry-rounds1，不要求test全通过或新增20条验收。先验证刚补的只读连接重试确实有效；若仍是整体main连接不可用，应做有界诊断并报告，不盲目反复启动或重放迁移。普通文件失败向后推进；deferred和partial后续单独恢复，不永久遗漏。

## 最新决议与授权边界（优先于下文历史记录）

用户最新决议：**接入 main 后边跑边处理，不再把 test 全部验收通过、速度问题全部查清或大文件处理完成作为启动门槛。** 失败不能阻塞后续正常文件；不再要求新跑 20 个 test/main 样本再另行确认全量。

1. 下一轮用户提供 main 配置，并在 Dashboard 设置单文件上限为 **5GB**。同时核对全局限制、桶限制、本机配置的字节数；清单最大文件 4,033,019,179 字节。当前只是用户承诺，尚未核验 main 的实际设置。
2. 采用**项目缓存/会话/归档判断隔离**，不先迁移 test 的 10–15 个对象。此实现已存在；test 数据原样保留，main 仅以 main 数据库和对象校验结果认定归档，不复用 test 成功标记。可复用仍在本机且校验一致的原件。
3. main 先以并发 **4** 推进正常大小的视频，按有限分段处理。当前默认阈值 128 MiB；更大的文件、3021 和已有部分文件单独列为 deferred，后续低并发/独立恢复。正常队列结束不等于全部 5,212 个完成；不能永久漏掉 deferred。
4. 单文件失败写任务状态（持有有效租约时）及本地持久化报告，继续下一个文件。本轮不无限重试，不让失败条目反复占住队首；之后按失败清单统一、有界重试。过期 token 不得强行提交或写 failed，过期租约由原有受限领取函数回收，本地报告仍必须保留失败证据。
5. Telegram 慢下载、Supabase 读回超时和续租失败继续处理，但不再以“所有问题都解决”为开始 main 的条件。大文件优化与普通队列推进分开安排，不新增第二个并行 worker 抢占同一项目锁。
6. 只有磁盘底线不足、持续限流、整个 main 连接不可用、凭据/权限不可用或同名资源不兼容等影响整个流水线的情况才暂停/停止新任务领取，并清楚报告；普通单文件错误继续向后推进。
7. **本次只更新交接和提供新窗口提示词，不启动 main。** 下一轮用户提供配置并使用所附启动提示词，即授权在确认目标后完成必要初始化和启动正常文件队列，不要再增加“test 全通过”或“再跑 20 条后确认”的门槛。若配置未明确或必要凭据缺失，只询问具体缺项，不读其他项目凭据补齐。

### 尚需补齐，不能冒称已经实现

- `run` 有限分段、项目隔离、停止逻辑、部分文件保护、阶段计时已实现。
- **统一失败重试入口尚未完成**：需支持从持久化报告选择可重试条目，尊重数据库退避/领取上限，区分单文件失败、过期运行记录和 deferred，不靠反复扫最早 N 条实现重试。
- **已有 `.part` 当前会被跳过而非自动续传**。重试方案需保留现有部分文件，不能删除/覆盖 3021 的 `.part`，也不能把 deferred 当成功。
- 每段仍一次创建全部条目任务，导入/状态查询虽有 6 连接信号量，但尚未限制实际在途条目数。可在 main 接入同轮补上，减少元数据请求与续租竞争；这不是已经证明的 lease_lost 根因。
- 真实读回超时/续租失败尚未修复，不得写成远端验收通过。接入时补齐必要的故障标记/恢复保障并跑针对性本机测试，同轮推进 main；不扩展为新的长期 test 验收阶段。

## 最新核实（2026-10-21，覆盖下文旧状态）

- 已实施：项目 ref 隔离本地缓存和 TUS 会话；流水线下载不受旧 test tombstone 误跳过；有限 `run` 分段入口；大文件/3021/已有部分文件 deferred；排队停止、全局短限流等待、上传线程协作取消；分阶段计时。001 新库执行入口已补上 003。无新增业务表或字段。
- 55440 已验证为本项目隔离实例，94 项本机测试通过，依赖检查通过。这不等于真实验收通过。
- 上次授权的精确六条（10、20、21、25、26、7）实际已结束，报告是 `incomplete`，896.8 秒：7 读回复验成功；10 已发完 TUS 42,334,479 字节，但完整读回 TimeoutError（读到 26,214,400 字节），未提交 object_path；20、21、25、26 仅下载约 4.3–4.6 MB，最终 lease_lost。没有 FloodWait。不要标记本轮验收通过。
- 报告：`/Users/qj/python_project/telegram-video-archive/archive/pipeline-batch-2026-10-20T14-37-37.747051+00-00.json`。
- 上次会话实时只读确认（不是本次重查）：项目进程锁空闲；当前配置仍匹配原 test；云端仍只有 10 条 object_path 已提交。10 upload running、20/21/25/26 download running 均为昨晚已过期租约，无活跃 worker；未人为改状态。
- 10 本地原件仍在。20/21/25/26 部分文件全部保留；3021 的 `.part` 仍为 110,100,480 字节。6558/6559 原件仍在且重新算 SHA-256 与记录相符。
- main 未启动；项目私有目录未发现单独的 main 配置文件，也未核验 main 迁移/权限/私有桶/5GB 限制。用户承诺接入前设置 5GB，不应写成已核验。
- 普通文件也发生真实超时/续租失败，技术风险仍在；用户已决定接受单文件失败后继续推进 main，不再把 test 完整通过作为前置条件。test 归档对象总预算不超过 15，勿为了重测另选更多视频。
- 尚未实施但需注意：每段仍会一次创建全部条目任务，数据库导入/状态查询仅受 6 连接信号量限制；进入 main 前可将实际在途条目数也限制，避免 100 条元数据请求抢占续租。没有证据证明这就是本次 lease_lost 的根因。

本文件是新会话的起点。上面事实是上次核查快照，不是实时状态，新窗口仍需先核实进程、配置目标、报告和文件。凭据只在 `/Users/qj/python_project/telegram-video-archive/.private/`，本文件不含任何秘密。下面早期记录与最新决议冲突时，以最新决议为准。

## 1. 背景

把 Telegram 短剧群（单一来源，约 5,212 个视频、约 608 GB）归档到 Supabase 私有桶，并为每个视频准备 AI 备用标题/标签/简介。

- 设计以 [SUPABASE_PIPELINE_DESIGN.md](/Users/qj/python_project/telegram-video-archive/SUPABASE_PIPELINE_DESIGN.md) 的 v2 为准：业务 schema `drama_archive`，只有 4 张表（dramas、videos、ai_results、pipeline_tasks），私有桶 `drama-videos`，对象路径 `videos/{video_id}/original`。
- 流程一：Telegram 下载 → 本地校验 → 上传桶 → 远端完整读回 SHA-256 → 事务提交 object_path → 释放本地缓存。流程二：AI（尚未接入真实模型）。
- Worker 通过 PostgreSQL 直连调用受限函数，不走 PostgREST。
- test 阶段已有基础闭环证据；最新阶段转为 main 接入准备，按顶部最新决议推进。

## 2. 涉及的文件

| 文件 | 作用 |
|---|---|
| [SUPABASE_PIPELINE_DESIGN.md](/Users/qj/python_project/telegram-video-archive/SUPABASE_PIPELINE_DESIGN.md) | v2 设计（权威） |
| [IMPLEMENTATION_RESULTS.md](/Users/qj/python_project/telegram-video-archive/IMPLEMENTATION_RESULTS.md) | 第一轮和第二轮（两样本真实上传）交付记录 |
| [PIPELINE.md](/Users/qj/python_project/telegram-video-archive/PIPELINE.md)、[README.md](/Users/qj/python_project/telegram-video-archive/README.md) | 运行、恢复、权限说明 |
| [migrations/001_pipeline.sql](/Users/qj/python_project/telegram-video-archive/migrations/001_pipeline.sql) | 四表、约束、RLS、事务函数（已改：并发导入冲突，见 §4） |
| [migrations/002_archived_messages.sql](/Users/qj/python_project/telegram-video-archive/migrations/002_archived_messages.sql) | 新增受限函数，返回云端已提交的 message_id |
| [migrations/003_import_video_concurrency.sql](/Users/qj/python_project/telegram-video-archive/migrations/003_import_video_concurrency.sql) | 修复并发导入同一剧时 dramas 主键冲突，只替换 import_video 函数体 |
| [pipeline.py](/Users/qj/python_project/telegram-video-archive/pipeline.py) | CLI：check、inventory、plan、verify-samples、migrate、bucket、import、samples、batch、run |
| [pipeline_batch.py](/Users/qj/python_project/telegram-video-archive/pipeline_batch.py) | 新增：有界批量调度（并发门、限流降并发、缓存暂停、清理） |
| [pipeline_common.py](/Users/qj/python_project/telegram-video-archive/pipeline_common.py) | 配置校验（已接受 Supavisor 池化主机）、稳定 UUID、快照导入 |
| [pipeline_db.py](/Users/qj/python_project/telegram-video-archive/pipeline_db.py) | 数据库调用、租约；新增连接重试和并发上限 6 |
| [pipeline_storage.py](/Users/qj/python_project/telegram-video-archive/pipeline_storage.py) | TUS 上传、流式读回 SHA-256 |
| [pipeline_cache.py](/Users/qj/python_project/telegram-video-archive/pipeline_cache.py) | 本地缓存日志、水位、保护、清理 |
| [archive.py](/Users/qj/python_project/telegram-video-archive/archive.py)、[core.py](/Users/qj/python_project/telegram-video-archive/core.py) | 原下载与校验；archive.py 已增加目标库判断、异步校验/状态钩子、部分文件保护和计时 |
| [tests/test_pipeline.py](/Users/qj/python_project/telegram-video-archive/tests/test_pipeline.py) | 新增测试（含本机 PostgreSQL 测试） |
| [tests/test_pipeline_scheduling.py](/Users/qj/python_project/telegram-video-archive/tests/test_pipeline_scheduling.py) | 项目隔离、调度停止、取消、分段失败推进和部分文件保留测试 |

私有文件（不要打印内容）：

- `.private/pipeline.json`：受限 worker 登录配置（当前指向 test 项目）。
- `.private/owner-pipeline.json`：owner 配置，只用于迁移和只读核对，不要用于 worker。
- `.private/railway-test-connection.json`：从 Railway 取得的 test 环境变量副本。
- `.private/implementation-backup/`：SQLite 一致性备份等。

## 3. 已完成并核实

**真实 Supabase（test）**

- 已创建 `drama_archive`（四表）、`drama_worker` 角色、受限登录 `drama_worker_login`、私有桶 `drama-videos`（500 MB 限制）。`pipeline.py check` 通过（restricted_login、四表 RLS、无客户端权限）。
- 套餐 pro 和全局单文件上限 500 MB 是用户在 Dashboard 确认的，程序不能自动验证（`live_project_verified` 恒为 false）。
- 两个样本 6558、6559 已上传，远端读回 SHA-256 一致。重复运行不重传；对象存在但数据库提交丢失时只补交数据库（受控故障注入，对象不变）。
- 小批量（并发 4，`--cleanup`）：消息 7、9、11、12、13、15、16、17 已上传、读回一致、本地清理，AI 为 pending。截至停止前桶内共 10 个对象（含两个样本）。

**本机测试**

- 带本机 PostgreSQL（端口 55440，库 drama_pipeline_test）：最后一次 94 项全部通过。命令见 §6；测试结果是历史证据，修改后按影响范围验证。

## 4. 本轮发现并修复的缺陷（均有测试）

1. 池化连接突发时被拒（视频 10 失败的原因）：60 次并发连接，原版失败 12 次，加重试后 0 次。现在只对"建立连接"重试 4 次（1/2/4 秒），并限制进程内并发 6。重试不会重发已执行的 SQL。
2. `error_code` 现在带模块名（如 `psycopg_OperationalError`），格式必须匹配 `last_error` 的 CHECK（`^[A-Za-z0-9_]{1,80}$`），否则 `fail` 写入会失败。
3. `--limit True` 被当成 1，已修。
4. 并发导入同一部剧的多个视频会因 `dramas_pkey` 冲突失败，003 已修并已应用到 test 库（owner、权限、签名不变）。
5. `select` 以前把本地 success 当作已归档，会漏掉"下载成功但上传未提交"的视频；现在以云端 `archived_messages()` 为准。

## 5. 尚未完成或不确定

- **视频 10**：最新 download succeeded；upload 为过期 running，TUS 发完但读回超时，object_path 未提交。本地原件仍在。
- **视频 20、21、25、26**：最新 download 为过期 running，本地各有约 4.3–4.6 MB 部分文件；失败为 lease_lost，没有完整文件。
- **3021（406 MiB）**：旧下载曾 TimeoutError 后重启。`.part` 110,100,480 字节保留；最新 download failed，新入口默认 deferred。
- 修复后已执行六条真实批量，但只一条已归档视频读回复验成功，整个批量 incomplete，详见顶部报告。
- 吞吐很低：8 个视频约 400 MB 用了 20 多分钟。按此速度 608 GB 不可接受，瓶颈没查清（下载速度、并发数、单文件超时）。
- 约 35 分钟内没有触发 Telegram 限流（只观察了并发 4，不代表安全）。
- 真实 Supabase 上的上传中断续传、租约过期、进程重启恢复没有验证，只有本机替身测试。
- 未实现：常驻调度、自动缓存清理调度、客户端签名/展示 API、真实 AI 提供商、灾难恢复流程。
- 原三样本门槛：用户已确认调整为已通过的 6558、6559，不再要求 3021。SQLite 的 `sample_ids` 和 `sample_passed=false` 没有改动，旧版 `archive.py` 的全量命令仍会因此拒绝。不要用旧命令跑全量。
- PIPELINE.md 和 IMPLEMENTATION_RESULTS.md 已补充新入口/验收记录；其中旧门槛或旧状态以本交接顶部最新决议为准。

## 6. 常用命令

```sh
# 本机 PostgreSQL 实例（端口 55440）需先在运行；测试代码会清空该测试库的 drama_archive，严禁指向 Supabase
DRAMA_TEST_DSN='host=127.0.0.1 port=55440 user=postgres dbname=drama_pipeline_test' /Users/qj/python_project/telegram-video-archive/.venv/bin/python -m unittest discover -s /Users/qj/python_project/telegram-video-archive/tests
# 离线查看正常/大文件分流，不联网
/Users/qj/python_project/telegram-video-archive/.venv/bin/python /Users/qj/python_project/telegram-video-archive/pipeline.py plan --max-video-mib 128
# 以下是 main 配置准备并核验后才执行的入口示意；配置文件路径以用户实际提供为准
/Users/qj/python_project/telegram-video-archive/.venv/bin/python /Users/qj/python_project/telegram-video-archive/pipeline.py --config /Users/qj/python_project/telegram-video-archive/.private/main-pipeline.json check
/Users/qj/python_project/telegram-video-archive/.venv/bin/python /Users/qj/python_project/telegram-video-archive/pipeline.py --config /Users/qj/python_project/telegram-video-archive/.private/main-pipeline.json run --batch-size 100 --concurrency 4 --max-video-mib 128 --cleanup
```

- 批量运行用 `batch --limit N`，N 为 1 到 100，报告写到 `archive/pipeline-batch-*.json`，读报告前先确认进程是否还在跑。
- 停止用 SIGTERM（不要强杀），租约过期后可恢复。同一时间只能有一个 worker，项目进程锁是 `.private/process.lock`。

## 7. 下一窗口执行顺序

1. 先读本文件及设计/代码/最新报告；只读核对进程锁、test/main 配置身份与原件/部分文件，不凭提示词假设完成。已有 worker 不擅自停止，不启动第二个。
2. 使用用户提供的本项目 main 配置，保存为独立的权限受限文件；不覆盖 test 配置，不把秘密放到命令参数、聊天输出或报告。owner 凭据仅初始化用，长期 worker 必须是受限登录。
3. 在确认 main 身份后只读检查现有 schema/角色/桶。缺少则按明确授权非破坏性应用 001–003、建立受限 worker/私有桶；同名不兼容停止报告，不能 DROP/覆盖。核验全局/桶/配置 5GB 能覆盖最大文件（全局限制仍需用户 Dashboard 确认）。
4. 同轮补齐上述失败清单/有界重试、过期租约恢复和必要在途限制，并做针对性本机测试；保留 SHA-256 读回→事务提交→清理的顺序，不为吞吐跳过完整性校验。不能要求 test 所有问题解决才继续。
5. 配置和基本权限可用后，同轮启动 main 正常文件队列，并发 4、有界缓存、有限分段。首段也是实际全量进程的一部分，不要求先停下来另等“20 条验收确认”。不迁 test 对象，不运行旧 archive.py 全量命令。
6. 单文件失败持续记账并向后推进；正常队列跑完按失败清单统一重试，遵守退避/尝试上限。大文件和 partial 单列后续恢复，慢速/超时诊断与主队列推进分开，始终只有一个 worker。
7. 报告 main 进程、报告路径、成功/失败/未开始/deferred 数量、阶段速度、缓存水位及停止原因，不能将 uploaded 当作 archived，也不能将正常队列完成当作全部完成。更新本交接和实施结果。

## 8. 约束

- 本轮仅交接，不启动 main；下一轮用户提供 main 配置并明确按本计划启动后，可初始化并启动正常文件全量，无需额外 test 验收门槛。常驻/增量服务仍不在范围。
- 不打印配置内容、DSN 密码、密钥、Telegram session、上传会话 URL 或签名链接。
- 不删除原始资料、校验记录或 3021 的 `.part`。6558、6559 的本地原件保留。
- 同名但不兼容的 schema、表、角色、桶或对象：停止并报告，不 DROP、不覆盖。
- 项目不是 Git 仓库，不自动初始化。
- 不新增业务表或字段，除非先说明阻塞。
