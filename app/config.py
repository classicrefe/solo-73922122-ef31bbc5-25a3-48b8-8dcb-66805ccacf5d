"""运行期配置：全部通过环境变量注入，由 docker-compose 提供默认值。"""
import os

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@db:5432/tryout",
)

# 模型上限
MAX_MODELS = 4
# 单条模型 SQL 语句超时（毫秒）
STATEMENT_TIMEOUT_MS = int(os.environ.get("STATEMENT_TIMEOUT_MS", "30000"))
# 节点并发上限（互不依赖的模型并行）
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "4"))
# 允许作为模型直接读取的源表
SOURCE_TABLES = ("sales", "refunds")
RUNNER_ROLE = "model_runner"

# ---------------------------------------------------------------------------
# 封存核账包（财务离线核账）相关配置
# ---------------------------------------------------------------------------
# 核账 ZIP 包落盘目录：必须位于持久化共享卷，多个 Web 实例/容器挂载同一卷
ARCHIVE_DIR = os.environ.get("ARCHIVE_DIR", "/data/archives")
# 只有包含该模型（净额结果）且批次成功的试跑才允许封存
ARCHIVE_REQUIRED_MODEL = "daily_net_sales"
# 封存事务等待行锁的上限（秒），超时给出明确冲突提示而非无限挂起
ARCHIVE_LOCK_TIMEOUT_SECONDS = int(os.environ.get("ARCHIVE_LOCK_TIMEOUT_SECONDS", "60"))
# 封存中间态在重启恢复时被判定为“上次进程中断”的宽限（秒）
ARCHIVE_STALE_SECONDS = int(os.environ.get("ARCHIVE_STALE_SECONDS", "30"))
