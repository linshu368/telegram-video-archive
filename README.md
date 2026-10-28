# Telegram 短剧处理

从已确认的 Telegram 来源收集视频信息，先按整部剧筛选，再交给现有下载、校验、上传和 AI 信息补全流程。代码按用途分组；下载队列按整部剧依次推进，剧内沿用现有并发。视频下载、校验、上传和缓存清理内核保持原实现。

## 从哪里开始

| 要做的事 | 入口 |
|---|---|
| 登录、确认来源、收集源视频信息 | `archive.py` |
| 查看筛选结果、运行现有归档与 AI 流程 | `pipeline.py` |
| 单独采集或检查 Telegram 话题元数据 | `topic_metadata.py` |
| 找到具体代码或辅助脚本 | [项目结构](/Users/qianpeijia/Documents/ChatGPT/短剧/telegram-video-archive/docs/STRUCTURE.md) |
| 查看参数、质量标准和运行边界 | [流程与操作](/Users/qianpeijia/Documents/ChatGPT/短剧/telegram-video-archive/docs/PIPELINE.md) |

三个根目录入口保留原命令和参数，实际实现已按职责放进 `video_archive/`。

## 当前流程：先筛选，再按整剧下载

完成既有登录与来源白名单确认后，使用同一数据目录执行：

```sh
# 仅获取信息，不下载视频或封面
"/Users/qianpeijia/Documents/ChatGPT/短剧/telegram-video-archive/.venv/bin/python" \
  "/Users/qianpeijia/Documents/ChatGPT/短剧/telegram-video-archive/archive.py" \
  --data "/Users/qianpeijia/Documents/ChatGPT/短剧/telegram-video-archive/archive" scan

# 离线判断整部剧，保存每部剧的结论与完整成员清单
"/Users/qianpeijia/Documents/ChatGPT/短剧/telegram-video-archive/.venv/bin/python" \
  "/Users/qianpeijia/Documents/ChatGPT/短剧/telegram-video-archive/pipeline.py" \
  --data "/Users/qianpeijia/Documents/ChatGPT/短剧/telegram-video-archive/archive" \
  plan --minimum-quality 720 \
  --report "/Users/qianpeijia/Documents/ChatGPT/短剧/telegram-video-archive/archive/drama-quality-plan.json"
```

一部剧以 Telegram 原始话题在扫描边界内可访问的全部视频为准。信息不完整的剧暂缓；包含横屏或方屏的剧排除；其余按既定分辨率和码率标准判断。合格剧的成员完整保留，少量画质异常只影响评分。具体规则见 [整剧筛选说明](/Users/qianpeijia/Documents/ChatGPT/短剧/telegram-video-archive/docs/PIPELINE.md)。

`plan/batch/run/retry` 都经过同一筛选关口。确认计划后，可限制本次只处理一部剧：

```sh
"/Users/qianpeijia/Documents/ChatGPT/短剧/telegram-video-archive/.venv/bin/python" \
  "/Users/qianpeijia/Documents/ChatGPT/短剧/telegram-video-archive/pipeline.py" \
  --data "/Users/qianpeijia/Documents/ChatGPT/短剧/telegram-video-archive/archive" \
  batch --drama-limit 1 --minimum-quality 720
```

每部剧的全部分段尝试结束后才进入下一部，剧内保留现有并发。单段上限提高到 500，默认仍为 100；分段不会拆散整剧调度。`--only/--first` 指定任一集会扩展到它所属的整部剧；`--limit` 是视频数量软目标，当前剧会完整纳入，达到目标后再停止接下一部；`--drama-limit` 直接限制剧数。普通单集失败会保留整剧未完成状态和待办，硬停止会停下队列。旧 `archive.py sample/backfill/resume/retry` 保留本地归档用途，不套用新筛选和整剧调度。

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

配置示例见 [config/examples](/Users/qianpeijia/Documents/ChatGPT/短剧/telegram-video-archive/config/examples)。真实配置、会话和运行数据仍使用原来的 `.private/`、`archive/`，不随源码迁移；不要把密钥写进示例或提交仓库。

## 文档

- [项目结构与脚本索引](/Users/qianpeijia/Documents/ChatGPT/短剧/telegram-video-archive/docs/STRUCTURE.md)
- [当前流程、筛选规则与操作](/Users/qianpeijia/Documents/ChatGPT/短剧/telegram-video-archive/docs/PIPELINE.md)
- [双流程与 Supabase 设计](/Users/qianpeijia/Documents/ChatGPT/短剧/telegram-video-archive/docs/SUPABASE_PIPELINE_DESIGN.md)
- [本地归档历史说明](/Users/qianpeijia/Documents/ChatGPT/短剧/telegram-video-archive/docs/history/LOCAL_ARCHIVE.md)
- [实施与验收历史记录](/Users/qianpeijia/Documents/ChatGPT/短剧/telegram-video-archive/docs/history/IMPLEMENTATION_RESULTS.md)
- [历史交接记录](/Users/qianpeijia/Documents/ChatGPT/短剧/telegram-video-archive/docs/history/PROGRESS_HANDOFF.md)

历史文档保留当时的绝对路径和运行事实，不代表本次已启动生产任务。

## 测试

使用已安装项目依赖的 Python 环境，并提供 FFmpeg/ffprobe：

```sh
"/Users/qianpeijia/Documents/ChatGPT/短剧/telegram-video-archive/.venv/bin/python" -m unittest discover \
  -s "/Users/qianpeijia/Documents/ChatGPT/短剧/telegram-video-archive/tests" -v
```

离线测试不会登录 Telegram 或启动生产下载。需要数据库或历史本地资料的测试，在缺少对应环境时明确跳过。
