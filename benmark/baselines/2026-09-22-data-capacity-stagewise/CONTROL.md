# 本次独立容量实验的执行约束

正式计划为四个 block、12 个 stage、48 轮。每次 `run_plan.py ROOT --repository ROOT/collector --stage NAME` 只执行被冻结顺序中的下一阶段；用新调用推进已正常完成的阶段，不重启已登记的阶段。阶段间等待时间保存在 execution 的时间戳中。

只有该控制器实际 `wait()` 获得的退出码、逐轮正常退出记录和无存活私有服务的检查同时通过，下一阶段才可执行。任何失败或未完成 claim 都停止该计划；不能删除 execution、lock、目录或日志来继续。观察工具超时不代表进程退出，应继续检查原工具句柄与实际进程身份。

锁文件永久保留，runner 继承锁描述符。controller 退出仅关闭自己的副本。controller→runner 与 runner→直接服务均通过冻结的 process_guard 安装父进程退出保护；强制终止不等于 WAL 排空，不能生成正常完成证据。

此私有控制器要求 Python 3.9+、Linux 5.3+ 和可用的 pidfd 支持，当前实际环境为 Python 3.12/Linux 6.8。`os.pidfd_open` 与 `signal.pidfd_send_signal` 的最低 Python 版本见 [Python os 文档](https://docs.python.org/3/library/os.html#os.pidfd_open) 和 [signal 文档](https://docs.python.org/3/library/signal.html#signal.pidfd_send_signal)。这不改变仓库通用采集程序的 Python 3.8+ 要求。

原实验的 A-only 请求时长校准和构建记录标为 prior 证据。本次重新核对并复制同一批执行文件，使用 500000 请求，不声称新建构建或重新校准。旧中断实验不恢复、不合并进本次比较。正式采样期间不运行项目构建、测试、其他实验或离线全量分析。
