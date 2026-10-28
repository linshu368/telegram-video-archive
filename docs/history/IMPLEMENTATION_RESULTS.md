# 本轮实际交付记录（2026-10-20）

## 最新交接：main 已初始化，但全量未真正运行（2026-10-21 16:31 北京时间）

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

## 最新验收结论（2026-10-21）

已实现项目缓存隔离、有限分段调度、普通/大文件分流、部分文件保护、排队停止和协作取消、分阶段传输计时；本机隔离 PostgreSQL 55440 下 94 项测试通过，依赖检查通过。

但六条真实 test 验收 **未通过**：报告 `/Users/qj/python_project/telegram-video-archive/archive/pipeline-batch-2026-10-20T14-37-37.747051+00-00.json` 为 incomplete（896.8 秒）。7 已归档读回复验成功；10 TUS 发完、完整读回 TimeoutError、归档未提交；20/21/25/26 下载仅约 4.3–4.6 MB 后 lease_lost。没有 FloodWait，不代表没有网络/续租问题。

2026-10-21 实时只读复查：无 worker，配置仍为原 test，云端已提交仍为 10 个视频。10 和四条下载任务保留已过期 running 租约，未做远端重置。所有部分文件、10 本地原件及两个保护样本均保留。main 尚未接入或启动。本轮结果不能作为 main 全量就绪证明；大文件可以后置，普通文件失败仍需修复和复验。

## 实现文件

- `/Users/qj/python_project/telegram-video-archive/migrations/001_pipeline.sql`：v2 四表、复合身份约束、CHECK/索引、RLS、受限角色、事务导入及租约/结果提交函数。
- `/Users/qj/python_project/telegram-video-archive/pipeline_common.py`：安全配置、只读快照导入、稳定 UUID、字段回退与严格 AI 输出校验、错误码脱敏。
- `/Users/qj/python_project/telegram-video-archive/pipeline_db.py`：独立数据库连接、事务函数调用、后台续租、租约丢失关闭提交。
- `/Users/qj/python_project/telegram-video-archive/pipeline_storage.py`：TUS 6 MiB 分块、会话恢复、不覆盖对象、流式远端读回 SHA-256、私有桶检查。
- `/Users/qj/python_project/telegram-video-archive/pipeline_cache.py`：独立 SQLite 日志、OS 读取/写入锁、预留/水位/剩余空间保护、条件清理和 tombstone。
- `/Users/qj/python_project/telegram-video-archive/pipeline.py`：配置检查、清单统计/导入、迁移、桶创建、两个样本重验/归档 CLI、下载接入和独立 AI 接口。
- `/Users/qj/python_project/telegram-video-archive/archive.py`：仅增加已云端归档的本地恢复保护，不改原下载媒体身份/TTL/General 校验。
- `/Users/qj/python_project/telegram-video-archive/tests/test_pipeline.py`：35 项新增测试（18 项纯离线、17 项 PostgreSQL）；原 40 项回归保留。
- `/Users/qj/python_project/telegram-video-archive/requirements.lock`：增加并安装锁定的 psycopg 3.2.10 / psycopg-binary 3.2.10 / typing_extensions 4.16.0。
- `/Users/qj/python_project/telegram-video-archive/pipeline.config.example.json`、`/Users/qj/python_project/telegram-video-archive/PIPELINE.md`、`/Users/qj/python_project/telegram-video-archive/README.md`：安全配置、运行恢复、权限与未验证边界。

`/Users/qj/python_project/telegram-video-archive/core.py` 与备份一致，未修改。项目没有 Git 仓库，未创建仓库、回退或覆盖无关文件。SQLite 原始 schema 未变，使用 backup API 生成一致性备份并验证 integrity_check=ok；备份保存在 `/Users/qj/python_project/telegram-video-archive/.private/implementation-backup/`。

## 本次实际命令与结果

1. 修改前回归：
   `/Users/qj/python_project/telegram-video-archive/.venv/bin/python -m unittest discover -s /Users/qj/python_project/telegram-video-archive/tests -v`
   **40 项通过**。
2. 最终完整测试：
   `DRAMA_TEST_DSN='host=127.0.0.1 port=55439 user=postgres dbname=drama_pipeline_test' /Users/qj/python_project/telegram-video-archive/.venv/bin/python -m unittest discover -s /Users/qj/python_project/telegram-video-archive/tests -v`
   **75 项通过，6.413 秒，无 skip**。
3. 最终无数据库配置回归：同第 1 条命令，**75 项发现，58 项通过、17 项 PostgreSQL 测试跳过，3.868 秒**。
4. `/Users/qj/python_project/telegram-video-archive/.venv/bin/python -m pip check`：`No broken requirements found.`
5. 新 Python 模块及新增测试执行 `compileall -q`：通过。
6. `/Users/qj/python_project/telegram-video-archive/.venv/bin/python /Users/qj/python_project/telegram-video-archive/pipeline.py inventory`：5,212 eligible 视频、891 dramas，不联网。
7. `/Users/qj/python_project/telegram-video-archive/.venv/bin/python /Users/qj/python_project/telegram-video-archive/pipeline.py verify-samples`：6558、6559 真实本地大小/hash/probe/完整解码通过。
8. `/Users/qj/python_project/telegram-video-archive/.venv/bin/python /Users/qj/python_project/telegram-video-archive/pipeline.py check`：按预期安全停止，`private_pipeline_config_missing`。
9. PostgreSQL 14.18 本机临时实例实际执行迁移；验证重复迁移拒绝覆盖、并发领取、过期/旧 token、跨来源 FK、角色权限、事务回滚、AI 失败保留旧值，以及真实清单 5,212 条两次幂等导入。

完整测试日志保存在 `/tmp/drama-pipeline-tests-final.log` 和 `/tmp/drama-offline-tests-final.log`。早期一次测试误重复导入原测试类，90 项计数无效；最终上面的 75 项已去重，并在后续新增测试后重新执行。

## 两个真实文件

| 消息 | 字节 | 稳定 video_id | 本次本地验证 | 云端验证 |
|---|---:|---|---|---|
| 6558 | 54,712,104 | a80210f3-3b95-5ff7-ad1d-7f04275df525 | 大小、SHA-256、ffprobe、完整解码通过 | 未执行 |
| 6559 | 56,592,060 | d9d54f14-11af-506e-95db-d1145367b77c | 大小、SHA-256、ffprobe、完整解码通过 | 未执行 |

本次验证时间：2026-10-20 08:56 UTC。完整摘要和媒体结果在 `/Users/qj/python_project/telegram-video-archive/archive/pipeline-local-acceptance.json`。两个原件未删除、未上传；清理/损坏/恢复测试只用临时测试文件。

真实远端数据库对象关联、AI pending 持久化、远端读回摘要、真实中断恢复都尚未验收。本机 PostgreSQL + 上传协议替身验证过这些逻辑，不等于真实 Supabase 成功。

## 原任务状态保留

最终只读复查：SQLite integrity_check=ok；6,590 消息、5,212 视频（2 success，5,210 pending）。3021 仍未完成。`sample_ids=[3021,6559,6558]`，`sample_passed=false`。未重写原 report/progress、未修改旧校验记录、未清理部分下载。未启动全量下载、批量上传或全库 AI。

## 缺项与下一步

1. 本机安全配置缺 `/Users/qj/python_project/telegram-video-archive/.private/pipeline.json`；需在本机填写目标 ref、region、plan、TLS 数据库 DSN、服务端 Storage key、实际全局/桶限制和两样本预算确认。无需在聊天提供任何秘密。
2. 两个文件都大于 Free 单文件范围；由用户确认已有可用套餐，不自动升级。确认区域存储和读回费用。
3. 远端使用 owner 执行一次非破坏性迁移，再设置受限 worker LOGIN；配置/核验私有桶，运行仅两样本命令并复跑验证。对象或 schema 冲突停止审查。
4. 本轮尚未实现通用批量调度 CLI、自动缓存清理调度、客户端展示/签名 API；现有模块和两样本 CLI 不能被描述为已投产全量系统。云端与本地完整性记录同时丢失的灾难恢复仍需专门操作流程。
5. 真实 TUS 服务行为、现代 secret key 的服务兼容性、目标实例默认权限需真实联调；上传测试当前用协议替身，非 live HTTP 服务。
6. 下载接入已有媒体校验且做了真实本机假 Telegram 客户端测试；尚未运行有界下载→上传双路长期压力/崩溃测试。全量开放前需完成这种小批量验收。新 worker 不允许并行绕过项目锁操作原 SQLite。
7. AI 提供商/型号/视频输入限制/素材政策/预算未确认，只有接口与严格测试替身。无真实 AI 调用。
8. 完成两个样本云端闭环后仍需停下等待用户确认；小批量/全量前明确处理原第三样本 3021 门槛。

启动、停止、恢复、安全配置的完整命令见 `/Users/qj/python_project/telegram-video-archive/PIPELINE.md`。
---

# 第二轮：test 项目真实 Supabase 验证（2026-10-20）

范围：仅 Supabase **test** 项目（凭据取自 Railway 部署的环境变量，已存入 `.private/` 的 0600 文件，不打印）。套餐 pro 与全局单文件上限 500MB 为**管理员 Dashboard 人工确认**，非远端自动验证；`check` 的 `live_project_verified` 仍为 false。

## 已改动
- `pipeline_common.py`：数据库主机校验接受官方 Supavisor 会话池（`aws-N-*.pooler.supabase.com:5432`，且登录名以 `.{project_ref}` 结尾），仍拒绝其他主机/端口/项目。`tests/test_pipeline.py` 新增对应用例。
- 新增本机私有文件：`.private/pipeline.json`（受限 worker 登录）、`.private/owner-pipeline.json`（仅迁移用 owner，勿用于 worker）。

## 真实 Supabase（test）已验证
- 非破坏性迁移：`drama_archive` 四表 + 七函数 + `drama_worker` 角色；受限登录 `drama_worker_login`（NOSUPERUSER/NOBYPASSRLS，连接上限 20）。`check` 权限审计通过。
- 私有桶 `drama-videos`（500MB 限制）。第一次创建被 413 拒绝，原因是 Dashboard 全局限制尚未生效，改后成功；未重试其他值。
- 6558、6559 经 TUS 上传，完整远端读回 SHA-256 与本地一致；对象路径 `videos/{video_id}/original`；upload succeeded 与 object_path 同事务；两视频 AI pending，ai_results 为 0 行（未调用模型）。
- 远端表计数：dramas 1、videos 2、pipeline_tasks 6；桶内恰 2 个对象，合计 111,304,164 字节。
- 重复运行：不重复建档，不重传，对象不变，仅读回校验。
- 对象已在而数据库提交丢失（用 owner 将 6558 的 object_path 清空、upload 置 pending，属**受控故障注入**）：重跑只补交数据库，对象 id/created_at/updated_at/ETag 不变。
- 原件保留，本地缓存缺失不触发 Telegram 下载（读回校验路径）。

## 仅本机/替身验证（未在真实 Supabase 复现）
上传中断续传、租约过期回收、旧 token 拒绝、进程重启恢复、缓存水位/清理、AI 失败保留旧值：由本机 PG 与 TUS 替身测试覆盖。

## 测试
带本机 PG（55440）84 项全通过；无 PG 62 通过、22 跳过。

## 未完成
批量调度 CLI、自动缓存清理、客户端签名 API、真实 AI 提供商、灾难恢复流程、小批量/全量。原三样本门槛未通过：3021 仍 pending，`sample_passed=false`。

---

# 第三轮：main 正常队列首次真正启动（2026-10-21）

范围：真实 Supabase **main** 项目 `wbtsfzozlmurljvglhpn`。此前 main 已初始化（001–003、四表、受限登录、私有桶）但两次 `run` 启动均在领取前因网络错误退出（`urllib_URLError`、`psycopg_OperationalError`），从未真正进入下载队列。本轮做了一次有界诊断（启动失败后连续 3 次只读 `check` 全部成功，判定为间歇性而非持续性连接问题）后再次启动，第三次尝试（PID 67355）确认真正进入下载/上传。

## 已验证
- 启动前 `check` 通过、离线 `plan` 核对正常队列 3720 项/214,266,407,115 字节、deferred 1492 项，与历史快照一致。
- `run --batch-size 100 --concurrency 4 --max-video-mib 128 --download-timeout 1800 --cache-high-gb 10 --retry-rounds 1 --cleanup` 真实启动，生成主报告与第一段 `segment-1` 报告（路径见 `PROGRESS_HANDOFF.md` 最新交接节），非离线/非替身。
- 第一段（100/3720）进行中快照：2 个单文件失败（http_RemoteDisconnected、urllib_URLError）后继续推进，未触发限流、未暂停领取。

## 未完成/未验证
- 正常队列远未跑完，deferred 1492 项本轮未处理。
- 吞吐仍偏低（历史已知问题），本轮未优化、未验证改善。
- 刚补的只读连接有界重试本轮首次在 main 真实验证到效果（`check` 间歇性失败后自行恢复），但连接错误根因（DNS/TLS/代理/上游）仍未确定。
- 统一失败重试入口、deferred/partial 单独恢复本轮仍未执行。
- 不能将本轮启动成功视为归档完成，也不能视为正常队列全部完成。
