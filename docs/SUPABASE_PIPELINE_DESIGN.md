# 视频双流程与 Supabase 精简设计

版本：v2，取代 v1；2026-10-20。仅设计，尚未创建远端表、桶或上传。

## 1. 保存范围与流程

只保留用户展示内容和两个流程必需的运行记录。不建设来源审计库、完整消息库、AI 历史版本库或费用分析库。现有本地原始资料不删除。

已扫描 5,212 个视频，约 608.4 GB。两个视频 6558、6559 下载和完整解码通过，任务已停止；第三个样本 3021 未完成，原三样本门槛仍未通过。

```text
流程一：Telegram → 2 路下载 → 本地校验 → 上传私有桶 → 确认归档 → 释放缓存
流程二：已归档视频 → 多模态模型 → 程序校验输出 → 保存备用标题/标签/简介
```

两个流程流水线运行。每个视频都生成备用信息，即使原始字段齐全。AI 失败只重试 AI；上传积压时暂停新增下载。本地保留有上限的缓存，不存全量。

## 2. 原表用途和去留

| 原表 | 原用途 | 精简处理 |
|---|---|---|
| sources | 来源群/频道、属性、采集边界 | 删除独立表；边界留本地，只保留必要来源 ID |
| topics | 话题名称、ID、改名观测、状态 | 改为 dramas，只保存短剧分组与名称 |
| source_messages | 全部原文、回复、服务消息、API 快照 | 不建云端表，已有资料留本地 |
| videos | 单条视频的原始内容与媒体属性 | 保留内容字段，删除不必要媒体属性，直接关联桶 |

一个话题可包含多个视频。dramas 是短剧目录，videos 是每个可播放文件，不能将两者误认为一对一。General 在导入前排除。

以已确认的话题作为短剧分组，不宣称全集完整、不猜集序。归属或名称缺失时留本地补查，不用单个视频的 AI 标题猜整部剧名。

## 3. 四张表，同一 schema

统一 schema：`drama_archive`。私有桶：`drama-videos`。

```text
dramas → videos → ai_results（一视频一份）
              → pipeline_tasks（下载、上传、AI 各一条）
videos.object_path → 私有桶文件
```

### 3.1 dramas：短剧目录

| 字段 | 类型 | 用途 |
|---|---|---|
| id | uuid PK | 短剧 ID |
| title | text NOT NULL | 展示用短剧名，来自话题名称 |
| source_id | bigint NOT NULL | Telegram 来源 ID，定位与去重 |
| topic_id | bigint NOT NULL | Telegram 话题 ID，分组与去重 |

唯一键 `(source_id, topic_id)`，同名不同话题不合并。身份字段不对用户展示。无需群名、目录审计、改名历史。

### 3.2 videos：原始信息与播放定位

| 字段 | 类型 | 用途 |
|---|---|---|
| id | uuid PK | 全流程 video_id |
| drama_id | uuid FK dramas.id NOT NULL | 所属短剧 |
| source_id | bigint NOT NULL | 来源身份与消息去重 |
| message_id | bigint NOT NULL | 恢复时定位源消息 |
| media_id | bigint NOT NULL | 防止消息附件替换后下载错文件 |
| original_title | text 可空 | 原始视频标题 |
| original_tags | text[] NOT NULL 默认空数组 | 原始标签 |
| original_description | text 可空 | 原始简介 |
| episode | text 可空 | 明确的集数/分段文字，不猜顺序 |
| object_path | text UNIQUE 可空 | 桶内路径，确认归档后填写 |

唯一键 `(source_id, message_id)`。dramas 增加 `(id, source_id)` 唯一约束，videos 使用 `(drama_id, source_id)` 复合外键防止跨来源错配。bigint 经过 JavaScript 时按十进制字符串传输，避免精度损失。

source_id/message_id/media_id 用于定位、恢复和去重，虽不展示但必须保留。文件名和标题不能替代身份。

从云端删除：original_field_provenance、original_filename、mime_type、expected_size_bytes、original_duration_seconds、original_width、original_height、video_evidence、scope_status、exclusion_reason。

大小、MIME、摘要和媒体校验在执行时使用，保留在本地记录或从文件/API 获取，不因此增加云端字段。时长、分辨率目前没有明确展示需求，不新增。话题名称用于 dramas.title，不自动冒充视频标题。

### 3.3 ai_results：只保存一份有效备用信息

| 字段 | 类型 | 用途 |
|---|---|---|
| video_id | uuid PK / FK videos.id | 一视频一行 |
| title | text 可空 | AI 备用标题 |
| tags | text[] NOT NULL 默认空数组 | AI 备用标签 |
| description | text 可空 | AI 备用简介 |

只保留以上四字段。不保存 input_strategy、generation_config、generation_key、raw_output、validation_status、validation_errors、is_current、usage、estimated_cost_usd，也不保存模型/提示词历史、输入资产版本。

- 模型配置放程序配置，合格输出才落库；失败原因写任务表。
- 重生成成功覆盖同一行，失败保留旧的有效结果，不保存历史版本。
- 不知道的内容可为空，不为补齐字段编造剧情。
- 一个 video_id 绑定不可变原视频。禁止替换视频却复用旧 AI 结果；明确更换时清空结果并重建任务。
- 不承诺从数据库回看历史模型参数与结果，这是本次精简的边界。

### 3.4 pipeline_tasks：流程运行记录

两个业务流程分 download、upload、ai 三个可重试步骤，共用一张表。

| 字段 | 类型 | 用途 |
|---|---|---|
| video_id | uuid FK videos.id | 对应视频 |
| stage | text CHECK | download / upload / ai |
| status | text CHECK | pending / running / succeeded / failed |
| attempts | integer 默认 0，非负 | 重试次数 |
| last_error | text 可空 | 最近脱敏错误 |
| lease_token | uuid 可空 | 并发领取凭据，防止旧任务提交 |
| lease_expires_at | timestamptz 可空 | 崩溃后回收任务 |
| updated_at | timestamptz NOT NULL | 最近状态更新时间 |

复合主键 `(video_id, stage)`，每视频最多三行。建立阶段/状态及运行中租约索引。最大重试和退避参数放程序配置，不逐行重复。

lease 两字段是并发恢复所需，不是审计扩展。领取用事务锁行或条件更新，长任务续租；提交必须匹配当前 token，完成时清除租约。

导入时创建三阶段 pending 记录。upload 需有效本地文件；ai 只有 upload succeeded 且 object_path 非空才可领取。对象路径与上传成功同事务提交；AI 内容与 AI 成功也同事务提交。成功任务默认不重做，明确重生成才重置 ai 状态。

## 4. 桶关联与恢复

桶名固定在服务端配置，不每行重复保存。对象路径：

```text
videos/{video_id}/original
```

对象键不要求扩展名，上传设置正确 Content-Type。路径不可静默覆盖；重复执行先核对已有文件。标题变化不影响路径。

- videos.id → object_path → 服务端签发短期链接；不保存永久公开地址或过期签名链接。
- Storage 系统表仍在 storage schema，四张业务表均在 drama_archive。
- 上传和数据库不是跨服务原子事务：对象已上传但状态未落库，核对后补写，不盲目重传。
- 本地 SQLite 保留缓存路径、大小、SHA-256、上传会话及校验记录。清理视频不删除 SQLite；本地记录丢失时需重新核验，不能仅凭对象路径认为一致。
- 远端存在、大小匹配不等于摘要校验通过。采用已验证的端到端校验；无可核实 checksum 时读回计算摘要，两个上传样本须实际读回验证。
- 校验确认、路径/状态落库、AI 待办存在且无本地读取者后才清理缓存。不要求 AI 已完成；后续可以从云端读取。
- 本地缺文件但上传已成功，不自动从 Telegram 重下。

## 5. 展示规则

短剧列表显示 dramas.title；视频详情由服务端按字段组装：

```text
标题：原始标题非空则使用，否则 AI 标题
标签：原始标签非空则使用，否则 AI 标签
简介：原始简介非空则使用，否则 AI 简介
集数：videos.episode
播放：object_path 对应的短期授权链接
```

字符串 trim，空串视为缺失；标签去空项、去重。两边都缺时标题/简介为空，标签为 []。逐字段回退，不整行替换、不自动混合标签、不覆盖原始字段。

不保存第三套 effective 字段，不强制创建 video_catalog 视图。仅 upload succeeded 且路径非空的条目提供播放；AI 未完成不影响原始信息齐全的视频展示。

## 6. 必要边界与实施

- 缓存高水位暂停下载、低水位恢复，并发预留计入总预算；上传失败不得删除可恢复文件。
- 私有桶、业务表 RLS；基础表首版仅可信 worker/服务端访问。客户端只经服务端取得展示字段与授权链接，不暴露任务表和 Telegram 定位信息。
- 配置自定义 schema 的 Data API 暴露及最小权限，密钥仅服务端保存。凭据、session、手机号、签名链接不写业务表或公开日志。
- 确认存储、模型、播放与读回流量费用；无需逐视频保存费用字段。模型/视频输入限制与采样策略在实现时确认，抽样不冒称完整观看。
- 本地原始资料继续保留，但不承诺 Supabase 可以恢复全部 Telegram 原文和历史。

实施顺序：检查现有项目 → 建四表/约束/权限/桶 → 导入已有目录与视频清单 → 两个样本上传及读回验证 → 验证重试/清理/崩溃恢复 → 处理原三样本门槛 → 小批量 → 全量；AI worker 可随后接入。

重点验收：重复导入不重复建档、上传失败不清理、清理后不误重下载、AI 失败不污染旧结果、逐字段回退、过期任务不能覆盖新任务。

本轮只精简文档，不删除本地数据、校验逻辑，不启动全量或配置 Supabase。