# agent-bridge-runner

在 Windows 上接收任务、驱动 CLI agent、把事件落盘。这是「桥」的执行端那一半。

## 背景

远端机器上的 CLI agent 通常只能在本地终端里跑，人一走开就看不见进度，也没法中途喊停。
本程序把任务提交、进度查询、取消和列举做成四个命令，让别的机器能通过 SSH 驱动它。
配套的调度端是 AstrBot 插件 astrbot_plugin_bridge_server。

## 设计要点

1. 任务不挂在 SSH 会话下。Windows 的 sshd 在会话结束时会终结整棵子进程树，
   所以这里为每个任务写一个 run.cmd，交给计划任务启动，进程便独立存活。
2. 所有事件按 seq 追加到 tasks/任务号/events.jsonl，watch 只按 seq 增量取，可重入。
3. 任务文本通过标准输入以 UTF-8 JSON 传入，绝不拼进命令行。
4. 输出强制 UTF-8，否则中文经 cmd 会按系统代码页编码，取事件的一端解析不了。

## 用法

    python runner.py submit              任务从 stdin 以 UTF-8 JSON 读入
    python runner.py watch 任务号 --after seq
    python runner.py cancel 任务号
    python runner.py list

## 事件格式

统一事件类型为 session、status、thinking、text、tool、tool_result、usage、final、error、raw。

## 适配器

- dsh：把 dsh --json 吐出的 NDJSON 事件翻成统一事件。
- codex：codex exec 目前没有结构化输出，先按行搬运。

## 配置

配置写在同目录的 policy.json。

| 字段 | 说明 |
| --- | --- |
| default_timeout_s | 单个任务的默认超时秒数 |
| max_output_chars | 单条事件文本的最大长度 |
| allowed_roots | 允许的工作目录白名单，提交任务时校验 |
| agents | 各 agent 的可执行文件与参数 |
| allowed_permissions | 允许的沙箱权限档位 |

要特别注意 allowed_roots，它是宽进策略，配得太宽等于不设限，请按需收紧。

## 安全

- 任务文本走标准输入，不拼进命令行，避免注入。
- 建议只让受控内网连到本机的 SSH。
- 沙箱档位里的 danger-full-access 等于把沙箱整个关掉，只在可信任务上使用。

## 依赖

Python 3，只用标准库，不需要第三方包。

## 许可

MIT
