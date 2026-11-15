"""封存核账包的端到端与可靠性测试。

覆盖：
- 封存资格（成功且含 daily_net_sales 净额结果）；
- ZIP 内容：sales/refunds 快照、各结果表 CSV、实际 SQL、依赖、批次号、逐文件 SHA-256；
- 金额不经过 float、标度无损；
- 幂等：重复点击/并发双实例只得到同一份有效包（同 id、同字节哈希）；
- 写盘/登记失败与进程中断：不出现“成功却无包”，重启可重试且字节一致；
- 已成功包的自愈合与不可变性：删文件/改源表/改模型都不改变登记摘要。
"""
import csv
import hashlib
import io
import os
import threading
import time
import zipfile
from decimal import Decimal

import psycopg2
import pytest

from conftest import conn as _new_conn
from conftest import restore_sources


def _conn():
    return _new_conn()


@pytest.fixture(autouse=True)
def clean_runs():
    def _clean():
        c = _conn()
        cur = c.cursor()
        cur.execute("SELECT snapshot_schema FROM public.runs")
        for (schema,) in cur.fetchall():
            if schema:
                cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        cur.execute("DELETE FROM public.run_events")
        cur.execute("DELETE FROM public.run_archives")
        cur.execute("DELETE FROM public.run_nodes")
        cur.execute("DELETE FROM public.runs")
        cur.execute("DELETE FROM public.models")
        cur.execute("ALTER SEQUENCE models_id_seq RESTART WITH 1")
        cur.execute(
            "INSERT INTO public.models (name, sql_text, upstream_ids) VALUES "
            "('daily_sales_clean', %s, '{}')",
            ("SELECT store_id, biz_date, SUM(amount) AS gross_amount "
             "FROM sales GROUP BY store_id, biz_date",),
        )
        cur.execute(
            "INSERT INTO public.models (name, sql_text, upstream_ids) "
            "SELECT 'daily_net_sales', %s, ARRAY[m.id] "
            "FROM public.models m WHERE m.name='daily_sales_clean'",
            ("SELECT s.store_id, s.biz_date, "
             "s.gross_amount - COALESCE(r.refund_amount, 0) AS net_amount "
             "FROM daily_sales_clean s LEFT JOIN ("
             "SELECT store_id, biz_date, SUM(refund_amount) AS refund_amount "
             "FROM refunds GROUP BY store_id, biz_date"
             ") r ON r.store_id=s.store_id AND r.biz_date=s.biz_date",),
        )
        c.commit()
        # 恢复源表基线（部分用例会上传 CSV 覆盖 sales），避免污染后续测试文件
        restore_sources(c.cursor())
        c.commit()
        c.close()
    _clean()
    yield
    _clean()


def _wait_terminal(client, run_id, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        d = client.get(f"/api/runs/{run_id}").json()
        if d["run"]["status"] in ("succeeded", "failed"):
            return d
        time.sleep(0.05)
    raise AssertionError("run did not finish in time")


def _make_succeeded_run(client):
    run_id = client.post("/api/runs").json()["run_id"]
    detail = _wait_terminal(client, run_id)
    assert detail["run"]["status"] == "succeeded"
    return run_id


def _seal(client, run_id):
    r = client.post(f"/api/runs/{run_id}/archive")
    assert r.status_code == 200, r.text
    return r.json()


def _open_zip(archive):
    return zipfile.ZipFile(archive["file_path"])


# ---------------------------------------------------------------------------
def test_seal_requires_succeeded_run_with_net_sales(client):
    # 运行中的批次不能封存
    run_id = client.post("/api/runs").json()["run_id"]
    r = client.post(f"/api/runs/{run_id}/archive")
    assert r.status_code == 409
    _wait_terminal(client, run_id)

    # 成功批次可封存
    a = _seal(client, run_id)
    assert a["status"] == "sealed"
    assert a["file_name"].endswith(".zip")
    assert len(a["file_sha256"]) == 64


def test_seal_rejected_without_net_sales_model(client):
    # 删掉净额模型，只保留清洗模型 -> 成功批次也不满足封存条件
    models = client.get("/api/models").json()
    net_id = next(m["id"] for m in models if m["name"] == "daily_net_sales")
    client.delete(f"/api/models/{net_id}")
    run_id = _make_succeeded_run(client)
    r = client.post(f"/api/runs/{run_id}/archive")
    assert r.status_code == 409 and "daily_net_sales" in r.json()["detail"]
    # 前置条件失败不应留下任何封存登记
    assert client.get(f"/api/runs/{run_id}/archive").status_code == 404


def test_failed_run_cannot_be_sealed(client):
    models = client.get("/api/models").json()
    up = next(m for m in models if m["name"] == "daily_sales_clean")
    client.patch(f"/api/models/{up['id']}/sql", json={
        "sql_text": "SELECT store_id, no_such_col FROM sales"})
    run_id = client.post("/api/runs").json()["run_id"]
    d = _wait_terminal(client, run_id)
    assert d["run"]["status"] == "failed"
    r = client.post(f"/api/runs/{run_id}/archive")
    assert r.status_code == 409


def test_archive_contents_and_manifest(client):
    run_id = _make_succeeded_run(client)
    a = _seal(client, run_id)
    zf = _open_zip(a)
    names = zf.namelist()
    expected = {
        "data/sales.csv", "data/refunds.csv",
        "data/result_daily_sales_clean.csv",
        "data/result_daily_net_sales.csv",
        "sql/executed_sql.sql", "dependencies.json",
        "SHA256SUMS.txt", "manifest.json",
    }
    assert expected.issubset(set(names))

    # 逐文件 SHA-256 与清单一致
    import json
    manifest = json.loads(zf.read("manifest.json"))
    assert manifest["batch_no"] == f"RUN-{run_id:06d}"
    assert manifest["run_id"] == run_id
    assert manifest["net_result_csv"] == "data/result_daily_net_sales.csv"
    for f in manifest["files"]:
        assert hashlib.sha256(zf.read(f["path"])).hexdigest() == f["sha256"]
    # SHA256SUMS 标准格式，财务可 sha256sum -c；覆盖除自身外的全部条目（含 manifest）
    sums = zf.read("SHA256SUMS.txt").decode()
    sum_paths = {line.split("  ", 1)[1] for line in sums.strip().splitlines()}
    assert "data/sales.csv" in sum_paths and "manifest.json" in sum_paths
    assert "SHA256SUMS.txt" not in sum_paths  # 校验和文件无法自引用
    assert len(sum_paths) == len(zf.namelist()) - 1
    # 清单内每个文件的哈希与校验和文本一致
    for line in sums.strip().splitlines():
        digest, path = line.split("  ", 1)
        assert hashlib.sha256(zf.read(path)).hexdigest() == digest

    # SQL 文件包含实际冻结 SQL 与依赖说明
    sql_text = zf.read("sql/executed_sql.sql").decode()
    assert "daily_net_sales" in sql_text and "gross_amount" in sql_text
    deps = json.loads(zf.read("dependencies.json"))
    net = next(n for n in deps["nodes"] if n["model_name"] == "daily_net_sales")
    assert net["upstream_model_names"] == ["daily_sales_clean"]

    # 行数登记正确
    assert a["row_counts"]["sales"] == 5
    assert a["row_counts"]["refunds"] == 3
    assert a["row_counts"]["daily_net_sales"] == 5


def test_amounts_exact_decimal_no_float_distortion(client):
    run_id = _make_succeeded_run(client)
    a = _seal(client, run_id)
    zf = _open_zip(a)

    net_rows = list(csv.DictReader(io.StringIO(
        zf.read("data/result_daily_net_sales.csv").decode())))
    row = next(r for r in net_rows
               if r["store_id"] == "S02" and r["biz_date"] == "2026-11-11")
    # 2100.25 - 100.25 = 2000.00，必须保留两位标度，绝不能出现 2000.0 或科学计数法
    assert row["net_amount"] == "2000.00"
    # 全部金额字符串都能被 Decimal 精确还原，且与数据库 NUMERIC 完全相等
    c = _conn(); cur = c.cursor()
    cur.execute(
        "SELECT s.store_id, s.biz_date, "
        "s.gross_amount - COALESCE(r.refund_amount, 0) "
        f"FROM {_snapshot_schema(run_id)}.daily_sales_clean s "
        f"LEFT JOIN {_snapshot_schema(run_id)}.refunds r "
        "ON r.store_id=s.store_id AND r.biz_date=s.biz_date "
        "WHERE s.store_id='S02' AND s.biz_date='2026-11-11'")
    db_value = cur.fetchone()[2]
    c.close()
    assert isinstance(db_value, Decimal)
    assert Decimal(row["net_amount"]) == db_value

    # 源快照 CSV 金额同样保留标度
    sales_rows = list(csv.DictReader(io.StringIO(
        zf.read("data/sales.csv").decode())))
    amount_by_id = {r["id"]: r["amount"] for r in sales_rows}
    assert amount_by_id["2"] == "980.50"
    assert amount_by_id["4"] == "2100.25"


def _snapshot_schema(run_id):
    c = _conn(); cur = c.cursor()
    cur.execute("SELECT snapshot_schema FROM public.runs WHERE id=%s", (run_id,))
    schema = cur.fetchone()[0]
    c.close()
    return schema


def test_idempotent_repeat_seal_returns_same_package(client):
    run_id = _make_succeeded_run(client)
    first = _seal(client, run_id)
    second = _seal(client, run_id)
    third = client.get(f"/api/runs/{run_id}/archive").json()
    # 同一条登记、同一个文件、同样的字节哈希
    assert first["id"] == second["id"] == third["id"]
    assert first["file_sha256"] == second["file_sha256"] == third["file_sha256"]
    # 数据库里每个批次只有一条封存记录
    c = _conn(); cur = c.cursor()
    cur.execute("SELECT count(*) FROM public.run_archives WHERE run_id=%s", (run_id,))
    assert cur.fetchone()[0] == 1
    c.close()


def test_concurrent_seals_produce_single_package(client):
    run_id = _make_succeeded_run(client)
    results, errors = [], []

    def worker():
        try:
            r = client.post(f"/api/runs/{run_id}/archive")
            results.append((r.status_code, r.json()))
        except Exception as exc:  # pragma: no cover - 并发下不应发生
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert all(code == 200 for code, _ in results)
    hashes = {body["file_sha256"] for _, body in results}
    ids = {body["id"] for _, body in results}
    assert len(hashes) == 1 and len(ids) == 1  # 只得到同一份有效包

    c = _conn(); cur = c.cursor()
    cur.execute("SELECT count(*), count(DISTINCT file_sha256) FROM public.run_archives "
                "WHERE run_id=%s", (run_id,))
    count, distinct = cur.fetchone()
    cur.execute("SELECT count(*) FROM public.run_archives WHERE status='sealed' "
                "AND run_id=%s", (run_id,))
    sealed = cur.fetchone()[0]
    c.close()
    assert count == 1 and distinct == 1 and sealed == 1


def test_full_zip_hash_matches_registry_and_download(client):
    run_id = _make_succeeded_run(client)
    a = _seal(client, run_id)
    with open(a["file_path"], "rb") as fh:
        assert hashlib.sha256(fh.read()).hexdigest() == a["file_sha256"]
    dl = client.get(f"/api/runs/{run_id}/archive/download")
    assert dl.status_code == 200
    assert dl.headers["content-type"] == "application/zip"
    assert a["file_name"] in dl.headers["content-disposition"]
    assert hashlib.sha256(dl.content).hexdigest() == a["file_sha256"]


def test_download_rejected_before_seal(client):
    run_id = _make_succeeded_run(client)
    r = client.get(f"/api/runs/{run_id}/archive/download")
    assert r.status_code == 404


def test_crash_mid_seal_recoverable_and_byte_stable(client, monkeypatch):
    """模拟写盘后、登记提交前进程中断：重启回收中间态，重试成功且确定性一致。"""
    from app import archives

    run_id = _make_succeeded_run(client)

    # 先正常封存拿到“确定性基线”字节
    baseline = _seal(client, run_id)
    baseline_sha = baseline["file_sha256"]

    # 人为制造崩溃现场：登记回 sealing（模拟 sealed 提交前进程死亡）
    c = _conn(); cur = c.cursor()
    cur.execute(
        "UPDATE public.run_archives SET status='sealing', "
        "updated_at=now() - interval '120 seconds' WHERE run_id=%s",
        (run_id,))
    c.commit(); c.close()

    # 未恢复前不能下载（不允许显示成功却拿不到完整可信包）
    r = client.get(f"/api/runs/{run_id}/archive/download")
    assert r.status_code == 404

    # 重启恢复：sealing -> failed
    recovered = archives.recover_archives_on_startup()
    assert run_id in recovered
    info = client.get(f"/api/runs/{run_id}/archive").json()
    assert info["status"] == "failed"

    # 重新封存：确定性重建，字节与基线完全一致
    again = _seal(client, run_id)
    assert again["status"] == "sealed"
    assert again["file_sha256"] == baseline_sha


def test_disk_failure_marks_failed_and_retry_succeeds(client, monkeypatch):
    """写盘失败时登记为 failed（绝不误报成功），恢复磁盘后重试可成功。"""
    from app import archives

    run_id = _make_succeeded_run(client)

    real_replace = os.replace
    calls = {"n": 0}

    def boom(src, dst):
        calls["n"] += 1
        raise OSError("模拟共享卷只读/写盘失败")

    monkeypatch.setattr(archives.os, "replace", boom)
    r = client.post(f"/api/runs/{run_id}/archive")
    assert r.status_code == 400 and "封存失败" in r.json()["detail"]
    monkeypatch.setattr(archives.os, "replace", real_replace)

    # 失败已登记，且没有成功包可下载
    info = client.get(f"/api/runs/{run_id}/archive").json()
    assert info["status"] == "failed"
    assert client.get(f"/api/runs/{run_id}/archive/download").status_code == 404

    # 重试成功
    a = _seal(client, run_id)
    assert a["status"] == "sealed"
    assert client.get(f"/api/runs/{run_id}/archive/download").status_code == 200


def test_self_heals_when_file_missing_or_corrupted(client):
    run_id = _make_succeeded_run(client)
    a = _seal(client, run_id)
    sha = a["file_sha256"]

    # 场景 1：文件被删，下载触发确定性重建
    os.remove(a["file_path"])
    dl = client.get(f"/api/runs/{run_id}/archive/download")
    assert dl.status_code == 200
    assert hashlib.sha256(dl.content).hexdigest() == sha
    assert os.path.isfile(a["file_path"])

    # 场景 2：文件被篡改，下载前校验失败并重建为正确字节
    with open(a["file_path"], "r+b") as fh:
        fh.seek(0)
        fh.write(b"CORRUPTED-CONTENT")
    dl = client.get(f"/api/runs/{run_id}/archive/download")
    assert dl.status_code == 200
    assert hashlib.sha256(dl.content).hexdigest() == sha


def test_successful_package_immune_to_later_source_and_model_changes(client):
    run_id = _make_succeeded_run(client)
    a = _seal(client, run_id)
    sealed_sha = a["file_sha256"]
    sealed_path = a["file_path"]

    # 封存后覆盖源表、修改模型 SQL，再触发一次“取包”，包内容不得改变
    new_sales = (
        "id,store_id,biz_date,amount\n"
        "99,S99,2030-01-01,1234567.89\n"
    ).encode()
    up = client.post(
        "/api/sources/sales/upload",
        files={"file": ("sales.csv", io.BytesIO(new_sales), "text/csv")})
    assert up.status_code == 200
    models = client.get("/api/models").json()
    clean = next(m for m in models if m["name"] == "daily_sales_clean")
    client.patch(f"/api/models/{clean['id']}/sql", json={
        "sql_text": "SELECT store_id, biz_date, SUM(amount) AS gross_amount "
                    "FROM sales GROUP BY store_id, biz_date ORDER BY 1,2"})

    again = _seal(client, run_id)  # 已 sealed 且文件完好 -> 原样返回
    assert again["file_sha256"] == sealed_sha
    assert os.path.getsize(sealed_path) == a["file_size"]

    # 包内仍是旧快照（5 行），不含新上传的 S99
    zf = _open_zip(again)
    sales = zf.read("data/sales.csv").decode()
    assert "S99" not in sales
    net = zf.read("data/result_daily_net_sales.csv").decode()
    # 旧快照净额结果保持不变
    assert "2000.00" in net


def test_no_temp_files_left_after_success(client):
    run_id = _make_succeeded_run(client)
    _seal(client, run_id)
    from app import archives as amod
    leftovers = [n for n in os.listdir(amod.ARCHIVE_DIR)
                 if n.startswith(".") and n.endswith(".tmp")]
    assert leftovers == []
    zips = [n for n in os.listdir(amod.ARCHIVE_DIR) if n.endswith(".zip")]
    assert len(zips) == 1
