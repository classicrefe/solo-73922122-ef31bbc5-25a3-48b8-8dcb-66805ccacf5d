"""pytest 共享夹具：真实 PostgreSQL + FastAPI TestClient。"""
import os
import tempfile

import psycopg2
import pytest
from fastapi.testclient import TestClient

# 测试环境无 /data 写权限，启动恢复需要一个可写的核账目录；
# 各用例再通过 monkeypatch 切换到自己的独立子目录。
os.environ.setdefault(
    "ARCHIVE_DIR", os.path.join(tempfile.gettempdir(), "tryout_test_archives")
)

SEED_SALES = [
    (1, "S01", "2026-11-10", 1200.00),
    (2, "S01", "2026-11-11", 980.50),
    (3, "S02", "2026-11-10", 1560.00),
    (4, "S02", "2026-11-11", 2100.25),
    (5, "S03", "2026-11-11", 430.00),
]
SEED_REFUNDS = [
    (1, "S01", "2026-11-11", 80.50),
    (2, "S02", "2026-11-10", 60.00),
    (3, "S02", "2026-11-11", 100.25),
]


def conn():
    c = psycopg2.connect(os.environ["DATABASE_URL"])
    c.autocommit = False
    return c


def restore_sources(cur):
    cur.execute("TRUNCATE public.sales")
    cur.executemany(
        "INSERT INTO public.sales (id, store_id, biz_date, amount) "
        "VALUES (%s,%s,%s,%s)", SEED_SALES,
    )
    cur.execute("TRUNCATE public.refunds")
    cur.executemany(
        "INSERT INTO public.refunds (id, store_id, biz_date, refund_amount) "
        "VALUES (%s,%s,%s,%s)", SEED_REFUNDS,
    )


@pytest.fixture(scope="session", autouse=True)
def _seed_sources():
    c = conn()
    restore_sources(c.cursor())
    c.commit()
    c.close()
    yield
    c = conn()
    restore_sources(c.cursor())
    c.commit()
    c.close()


@pytest.fixture(scope="session")
def client():
    from app.main import app
    with TestClient(app) as cl:
        yield cl


@pytest.fixture(autouse=True)
def _isolate_archive_dir(tmp_path, monkeypatch):
    """每个测试使用独立的核账包目录，避免共享卷上的文件互相干扰。"""
    from app import archives, config

    archive_dir = tmp_path / "archives"
    archive_dir.mkdir()
    monkeypatch.setattr(config, "ARCHIVE_DIR", str(archive_dir))
    monkeypatch.setattr(archives, "ARCHIVE_DIR", str(archive_dir))
    yield str(archive_dir)
