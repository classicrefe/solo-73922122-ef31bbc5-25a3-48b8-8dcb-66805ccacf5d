"""封存核账包（财务离线核账）。

职责
----
把一个**成功且包含 ``daily_net_sales`` 净额结果**的试跑批次封存为 ZIP 核账包：

- 快照 schema 内的 ``sales`` / ``refunds`` 全量快照、各成功模型结果表（含净额结果）
  全量导出为 CSV；
- 附实际执行的 SQL（节点冻结版本）、节点依赖关系、批次号、每个文件的行数与
  SHA-256 清单（``manifest.json`` + 标准 ``SHA256SUMS.txt``）；
- 金额列以 PostgreSQL ``NUMERIC -> decimal.Decimal`` 原样字符串化，全程不经过
  ``float``，杜绝浮点失真；
- ZIP 字节流整体计算 SHA-256，落盘到持久共享卷 ``ARCHIVE_DIR``，并在
  ``public.run_archives`` 登记状态与包摘要。

可靠性保证
~~~~~~~~~~
1. 以 ``runs`` 行级 ``FOR UPDATE`` 锁按批次串行化：两个 Web 实例同时封存同一批次、
   或用户重复点击，第二个请求阻塞后只会看到已 ``sealed`` 的同一份包并幂等返回，
   全系统只产出一份有效包。
2. 先写临时文件 -> 同卷原子 ``os.replace`` -> 同事务登记 ``sealed``。任一步失败或
   进程中断，登记都不会变成成功；重启后中间态被回收为 ``failed``，可重新封存。
3. ZIP 按固定条目顺序、固定时间戳、固定压缩参数确定性构建；快照 schema 不可变，
   因此中断重试得到的字节与哈希始终一致，已成功包的字节永远等于登记摘要。
4. 下载前重新计算文件 SHA-256 与登记值比对；缺失/被篡改时确定性重建自愈合，
   校验仍不过则拒绝下载——绝不会“显示成功却下载不到完整包”。
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import os
import uuid
from datetime import date, datetime
from decimal import Decimal

import psycopg2
import psycopg2.extras
from psycopg2 import sql as pgsql

from . import db as dbmod
from .config import (
    ARCHIVE_DIR,
    ARCHIVE_LOCK_TIMEOUT_SECONDS,
    ARCHIVE_REQUIRED_MODEL,
    ARCHIVE_STALE_SECONDS,
    SOURCE_TABLES,
)
from .sql_guard import validate_identifier

log = logging.getLogger("archives")

# 确定性 ZIP：所有条目使用统一的 DOS 时间戳与权限位，保证跨次重建字节一致
_ZIP_DATE_TIME = (1980, 1, 1, 0, 0, 0)
_ZIP_EXTERNAL_ATTR = (0o644 & 0xFFFF) << 16
# 表内固定导出的源快照（逻辑名 -> 快照内表名）
_SOURCE_FILES = tuple((t, t) for t in SOURCE_TABLES)


class ArchiveError(ValueError):
    """封存/下载业务错误（映射为 4xx）。"""


class ArchiveValidationError(ArchiveError):
    """封存前置条件不满足（不写 failed 登记，批次状态本就不该被封存）。"""


class ArchiveIntegrityError(RuntimeError):
    """已登记包与磁盘字节不一致且无法自愈合（映射为 5xx）。"""


# ---------------------------------------------------------------------------
# 表结构与启动恢复
# ---------------------------------------------------------------------------
_table_ready = False


def ensure_archive_table() -> None:
    """幂等建表：对已存在的旧库（init.sql 未重跑）也能补齐核账登记表。

    进程内只执行一次 DDL，避免高频封存路径上 ``CREATE INDEX`` 互相阻塞。
    """
    global _table_ready
    if _table_ready:
        return
    ddl = """
    CREATE TABLE IF NOT EXISTS public.run_archives (
        id             SERIAL PRIMARY KEY,
        run_id         INTEGER NOT NULL UNIQUE REFERENCES public.runs(id) ON DELETE CASCADE,
        status         TEXT NOT NULL DEFAULT 'sealing'
                       CHECK (status IN ('sealing','sealed','failed')),
        file_path      TEXT,
        file_name      TEXT,
        file_size      BIGINT,
        file_sha256    TEXT,
        row_counts     JSONB NOT NULL DEFAULT '{}'::jsonb,
        manifest       JSONB NOT NULL DEFAULT '{}'::jsonb,
        error          TEXT,
        created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """
    with dbmod.conn_ctx() as conn, conn.cursor() as cur:
        cur.execute(ddl)
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_run_archives_status "
            "ON public.run_archives(status)"
        )
        conn.commit()
    _table_ready = True


def ensure_archive_dir() -> str:
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    return ARCHIVE_DIR


def recover_archives_on_startup() -> list[int]:
    """回收上次进程中断残留的 sealing 登记，置为 failed 以便用户重试。

    只回收超过宽限期的记录，避免误伤多实例部署下另一实例正在进行的封存。
    """
    ensure_archive_table()
    ensure_archive_dir()
    with dbmod.conn_ctx() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE public.run_archives
            SET status='failed',
                error=COALESCE(error, %s),
                updated_at=now()
            WHERE status='sealing'
              AND updated_at < now() - (%s || ' seconds')::interval
            RETURNING id, run_id
            """,
            (
                "服务重启：上次封存过程中断，可重新封存",
                str(ARCHIVE_STALE_SECONDS),
            ),
        )
        stale = cur.fetchall()
        conn.commit()
    # 清理共享卷上可能残留的临时文件（正式包由确定性重建覆盖，无需删除）
    try:
        for name in os.listdir(ARCHIVE_DIR):
            if name.startswith(".") and name.endswith(".tmp"):
                try:
                    os.remove(os.path.join(ARCHIVE_DIR, name))
                except OSError:
                    pass
    except FileNotFoundError:
        pass
    if stale:
        log.warning("回收中断的封存登记: %s", stale)
    return [r[1] for r in stale]


# ---------------------------------------------------------------------------
# 取值格式化：金额不经过 float
# ---------------------------------------------------------------------------
def _format_value(value):
    if value is None:
        return ""
    # 关键防线：NUMERIC 经 psycopg2 以 Decimal 返回，直接字符串化保留标度，
    # 例如 Decimal('2000.00') -> '2000.00'。任何 float 都视为程序缺陷直接报错。
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, str)):
        return str(value)
    if isinstance(value, float):
        raise ArchiveIntegrityError(
            "数据库返回了 float 类型，拒绝以浮点形式写出金额/数值，请检查驱动类型"
        )
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    # UUID 等其余类型统一字符串化
    return str(value)


class _HashingWriter:
    """转发二进制写并同时累计 SHA-256 与字节数。"""

    def __init__(self, sink):
        self._sink = sink
        self.hasher = hashlib.sha256()
        self.size = 0

    def write(self, data: bytes) -> int:
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("归档写入层只接受字节")
        n = self._sink.write(data)
        self.hasher.update(data[:n])
        self.size += n
        return n

    def flush(self):
        self._sink.flush()

    def close(self):
        self._sink.close()


def _zip_info(arcname: str):
    import zipfile

    info = zipfile.ZipInfo(arcname, date_time=_ZIP_DATE_TIME)
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = _ZIP_EXTERNAL_ATTR
    info.create_system = 3  # UNIX，固定值保证确定性
    return info


def _write_bytes_entry(zf, arcname: str, payload: bytes) -> dict:
    raw = zf.open(_zip_info(arcname), "w")
    writer = _HashingWriter(raw)
    writer.write(payload)
    writer.flush()
    raw.close()  # 关闭 ZipExtFile 以完成条目收尾（哈希写入器不传播 close）
    return {
        "path": arcname,
        "sha256": writer.hasher.hexdigest(),
        "size_bytes": writer.size,
    }


def _dump_json(payload) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, default=str)
        + "\n"
    ).encode("utf-8")


# ---------------------------------------------------------------------------
# 单表 CSV 导出
# ---------------------------------------------------------------------------
def _export_table_csv(zf, arcname: str, qualified_table: str, conn,
                      export_name: str) -> dict:
    with conn.cursor() as meta_cur:
        meta_cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
            qualified_table.split(".", 1),
        )
        columns = [r[0] for r in meta_cur.fetchall()]
    if not columns:
        raise ArchiveError(f"结果表 {qualified_table} 不存在或无列，无法封存")

    order_by = pgsql.SQL(", ").join(pgsql.Identifier(c) for c in columns)
    query = pgsql.SQL("SELECT * FROM {} ORDER BY {}").format(
        pgsql.SQL(qualified_table), order_by
    )

    # 服务端游标分批拉取，避免大表一次性占满内存
    named = conn.cursor(name=f"export_{export_name}_{uuid.uuid4().hex[:8]}")
    named.itersize = 2000
    named.execute(query)

    entry = _zip_info(arcname)
    raw = zf.open(entry, "w")
    sink = _HashingWriter(raw)
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(columns)
    row_count = 0
    try:
        while True:
            batch = named.fetchmany(2000)
            if not batch:
                break
            for row in batch:
                writer.writerow([_format_value(v) for v in row])
                row_count += 1
            # 显式编码后写入字节层，避免 TextIOWrapper 接管底层句柄的关闭语义
            sink.write(buffer.getvalue().encode("utf-8"))
            buffer.seek(0)
            buffer.truncate(0)
    finally:
        named.close()
        buffer.close()
        raw.close()  # 关闭 ZipExtFile 完成条目收尾

    return {
        "path": arcname,
        "sha256": sink.hasher.hexdigest(),
        "size_bytes": sink.size,
        "rows": row_count,
        "columns": columns,
    }


# ---------------------------------------------------------------------------
# 封存主流程
# ---------------------------------------------------------------------------
def _file_name_for(run_id: int) -> str:
    return f"run_{run_id:06d}_audit_package.zip"


def _build_package(run_id: int, run: dict, nodes: list[dict], conn) -> dict:
    """在当前事务/连接内构建确定性 ZIP，返回登记摘要（不含 ZIP 自身哈希）。"""
    import zipfile

    schema = run["snapshot_schema"]
    validate_identifier(schema)
    archive_dir = ensure_archive_dir()
    file_name = _file_name_for(run_id)
    final_path = os.path.join(archive_dir, file_name)
    temp_path = os.path.join(archive_dir, f".{file_name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")

    batch_no = f"RUN-{run_id:06d}"

    succeeded = [n for n in nodes if n["status"] == "succeeded"]
    net_node = next(
        (n for n in succeeded if n["model_name"] == ARCHIVE_REQUIRED_MODEL), None
    )
    if net_node is None:
        raise ArchiveError(
            f"批次没有成功的 {ARCHIVE_REQUIRED_MODEL} 净额结果，不满足封存条件"
        )

    file_entries: list[dict] = []
    row_counts: dict[str, int] = {}

    zip_sink = _HashingWriter(open(temp_path, "wb"))
    try:
        with zipfile.ZipFile(zip_sink, mode="w", compression=zipfile.ZIP_DEFLATED,
                             compresslevel=9) as zf:
            # 1) 源快照 CSV：sales / refunds
            for logical, table_name in _SOURCE_FILES:
                arc = f"data/{logical}.csv"
                info = _export_table_csv(
                    zf, arc, f"{schema}.{table_name}", conn, logical
                )
                file_entries.append(info)
                row_counts[logical] = info["rows"]

            # 2) 成功节点结果表 CSV（含 daily_net_sales 净额结果，全量）
            for node in sorted(succeeded, key=lambda n: n["position"]):
                model = node["model_name"]
                validate_identifier(model)
                arc = f"data/result_{model}.csv"
                info = _export_table_csv(
                    zf, arc, node["result_table"], conn, f"result_{model}"
                )
                info["model_name"] = model
                file_entries.append(info)
                row_counts[model] = info["rows"]

            # 3) 实际执行 SQL（节点冻结的 SQL 文本，即真正在该批次执行的版本）
            sql_parts = [
                f"-- 核账批次号: {batch_no}",
                f"-- 试跑 run_id: {run_id}",
                f"-- 快照 schema: {schema}",
                f"-- 源数据版本: {run['source_version']}",
                "-- 以下 SQL 为各节点在本批次实际执行的冻结版本"
                "（重试成功的节点为重试时刷新的版本）。",
                "",
            ]
            for node in sorted(nodes, key=lambda n: n["position"]):
                ups = _upstream_model_names(node, nodes)
                sql_parts.extend([
                    "-- " + "=" * 70,
                    f"-- 模型: {node['model_name']}（节点 #{node['id']}，"
                    f"拓扑位置 {node['position']}，状态 {node['status']}）",
                    f"-- 依赖模型: {', '.join(ups) if ups else '（无）'}",
                    f"-- 结果表: {node['result_table'] or '（无）'}",
                    "-- " + "=" * 70,
                    node["sql_text"].rstrip().rstrip(";") + ";",
                    "",
                ])
            file_entries.append(
                _write_bytes_entry(
                    zf, "sql/executed_sql.sql", "\n".join(sql_parts).encode("utf-8")
                )
            )

            # 4) 依赖关系
            deps_payload = {
                "batch_no": batch_no,
                "run_id": run_id,
                "snapshot_schema": schema,
                "source_version": run["source_version"],
                "nodes": [
                    {
                        "node_id": n["id"],
                        "model_id": n["model_id"],
                        "model_name": n["model_name"],
                        "position": n["position"],
                        "status": n["status"],
                        "upstream_node_ids": list(n["upstream_ids"] or []),
                        "upstream_model_names": _upstream_model_names(n, nodes),
                        "result_table": n["result_table"],
                    }
                    for n in sorted(nodes, key=lambda n: n["position"])
                ],
            }
            file_entries.append(
                _write_bytes_entry(
                    zf, "dependencies.json", _dump_json(deps_payload)
                )
            )

            # 5) 清单（包内不含 ZIP 自身哈希与时间戳，保证跨次重建字节一致；
            #    ZIP 级哈希与封存时间登记在数据库）。先生成清单，使其能进入校验和。
            manifest = {
                "batch_no": batch_no,
                "run_id": run_id,
                "snapshot_schema": schema,
                "source_version": run["source_version"],
                "required_model": ARCHIVE_REQUIRED_MODEL,
                "net_result_csv": f"data/result_{ARCHIVE_REQUIRED_MODEL}.csv",
                "files": sorted(file_entries, key=lambda e: e["path"]),
            }
            file_entries.append(
                _write_bytes_entry(zf, "manifest.json", _dump_json(manifest))
            )

            # 6) 逐文件 SHA-256 标准文本清单（财务可用 sha256sum -c 离线核验）。
            #    校验和文件无法自引用，覆盖除自身外的全部条目（含 manifest.json）。
            sums_payload = "".join(
                f"{e['sha256']}  {e['path']}\n"
                for e in sorted(file_entries, key=lambda e: e["path"])
            ).encode("utf-8")
            file_entries.append(
                _write_bytes_entry(zf, "SHA256SUMS.txt", sums_payload)
            )
    except BaseException:
        zip_sink.close()
        try:
            os.remove(temp_path)
        except OSError:
            pass
        raise

    zip_sink.close()
    # 原子落位：同卷 rename，要么看到旧包要么看到完整新包，不会出现半截 ZIP
    os.replace(temp_path, final_path)

    return {
        "file_path": final_path,
        "file_name": file_name,
        "file_size": zip_sink.size,
        "file_sha256": zip_sink.hasher.hexdigest(),
        "row_counts": row_counts,
        "manifest": manifest,
        "batch_no": batch_no,
    }


def _upstream_model_names(node: dict, nodes: list[dict]) -> list[str]:
    by_id = {n["id"]: n["model_name"] for n in nodes}
    return [by_id[uid] for uid in (node["upstream_ids"] or []) if uid in by_id]


def _load_run_and_nodes(conn, run_id: int):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM public.runs WHERE id=%s FOR UPDATE", (run_id,))
        run = cur.fetchone()
        if run is None:
            raise ArchiveError(f"试跑 #{run_id} 不存在")
        cur.execute(
            "SELECT * FROM public.run_nodes WHERE run_id=%s ORDER BY position",
            (run_id,),
        )
        nodes = [dict(r) for r in cur.fetchall()]
        cur.execute(
            "SELECT * FROM public.run_archives WHERE run_id=%s FOR UPDATE",
            (run_id,),
        )
        archive = cur.fetchone()
    return dict(run), nodes, (dict(archive) if archive else None)


def seal_run_archive(run_id: int) -> dict:
    """封存（或幂等取回）某批次的核账包。返回登记行 dict。

    整个“校验 -> 写盘 -> 登记”过程都在持有 ``runs`` 行级锁的**同一事务**内，
    同一批次的并发封存被串行化，第二个请求只会看到第一个请求提交的 sealed 包。
    """
    ensure_archive_dir()
    with dbmod.conn_ctx() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SET LOCAL lock_timeout = {int(ARCHIVE_LOCK_TIMEOUT_SECONDS)}000"
                )
            run, nodes, archive = _load_run_and_nodes(conn, run_id)

            if run["status"] != "succeeded":
                raise ArchiveValidationError(
                    f"只有成功的试跑可以封存，当前批次状态为 {run['status']}"
                )
            if not any(
                n["status"] == "succeeded" and n["model_name"] == ARCHIVE_REQUIRED_MODEL
                for n in nodes
            ):
                raise ArchiveValidationError(
                    f"批次未产出成功的 {ARCHIVE_REQUIRED_MODEL} 净额结果，不能封存"
                )

            # 已有 sealed 包：先校验磁盘字节；完好则幂等返回同一份包
            if archive and archive["status"] == "sealed":
                if _file_matches(archive["file_path"], archive["file_sha256"]):
                    return _serialized(archive)
                log.warning(
                    "run %s 已登记包缺失/被篡改，确定性重建: %s",
                    run_id, archive.get("file_path"),
                )
                # 落到下面的重建分支（行已被 FOR UPDATE 锁住）

            # 插入或回收登记为中间态 sealing（不提交，锁不释放）
            with conn.cursor() as cur:
                if archive is None:
                    cur.execute(
                        "INSERT INTO public.run_archives (run_id, status) "
                        "VALUES (%s, 'sealing')",
                        (run_id,),
                    )
                else:
                    cur.execute(
                        "UPDATE public.run_archives SET status='sealing', "
                        "error=NULL, updated_at=now() WHERE run_id=%s",
                        (run_id,),
                    )

            # 在持锁事务内完成确定性写盘（ZIP 不写数据库，只读快照与结果表）
            built = _build_package(run_id, run, nodes, conn)

            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    UPDATE public.run_archives
                    SET status='sealed', file_path=%s, file_name=%s, file_size=%s,
                        file_sha256=%s, row_counts=%s, manifest=%s, error=NULL,
                        updated_at=now()
                    WHERE run_id=%s
                    RETURNING *
                    """,
                    (
                        built["file_path"], built["file_name"], built["file_size"],
                        built["file_sha256"],
                        psycopg2.extras.Json(built["row_counts"]),
                        psycopg2.extras.Json(built["manifest"]),
                        run_id,
                    ),
                )
                row = cur.fetchone()
                cur.execute(
                    "INSERT INTO public.run_events (run_id, event_type, message) "
                    "VALUES (%s, 'archive_sealed', %s)",
                    (run_id,
                     f"核账包已封存: {built['file_name']} "
                     f"(sha256={built['file_sha256'][:16]}…, "
                     f"{built['file_size']} 字节)"),
                )
                conn.commit()
            return _serialized(row)
        except ArchiveValidationError:
            # 前置条件不满足：回滚即可，不产生封存登记
            conn.rollback()
            raise
        except ArchiveError:
            # 写盘/构建类业务失败：回滚后单独把登记置 failed（若已存在 sealing 行）
            conn.rollback()
            _mark_failed(run_id, None)
            raise
        except psycopg2.errors.LockNotAvailable as exc:
            conn.rollback()
            raise ArchiveError(
                "该批次正在被另一个请求封存，请稍后重试（同一批次只生成一份包）"
            ) from exc
        except Exception as exc:
            conn.rollback()
            msg = str(exc).strip()[:2000] or exc.__class__.__name__
            _mark_failed(run_id, msg)
            raise ArchiveError(f"封存失败: {msg}") from exc


def _mark_failed(run_id: int, error: str | None) -> None:
    """独立短事务把登记置为 failed（原事务可能已回滚，不可复用）。"""
    try:
        with dbmod.conn_ctx() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.run_archives (run_id, status, error)
                VALUES (%s, 'failed', %s)
                ON CONFLICT (run_id) DO UPDATE
                SET status='failed',
                    error=COALESCE(EXCLUDED.error, run_archives.error),
                    updated_at=now()
                """,
                (run_id, error or "封存过程中发生错误，可重试"),
            )
            conn.commit()
    except Exception:
        log.exception("无法写入封存失败状态 run=%s", run_id)


# ---------------------------------------------------------------------------
# 查询与下载
# ---------------------------------------------------------------------------
def _serialized(row) -> dict:
    d = dict(row)
    for k in ("created_at", "updated_at"):
        if d.get(k):
            d[k] = d[k].isoformat()
    return d


def get_archive(run_id: int) -> dict | None:
    with dbmod.conn_ctx() as conn:
        row = dbmod.fetch_one(
            conn, "SELECT * FROM public.run_archives WHERE run_id=%s", (run_id,)
        )
    return _serialized(row) if row else None


def list_archives(limit: int = 100) -> list[dict]:
    with dbmod.conn_ctx() as conn:
        rows = dbmod.fetch_all(
            conn,
            "SELECT * FROM public.run_archives ORDER BY run_id DESC LIMIT %s",
            (max(1, min(limit, 500)),),
        )
    return [_serialized(r) for r in rows]


def _file_matches(path: str | None, expected_sha: str | None) -> bool:
    if not path or not expected_sha or not os.path.isfile(path):
        return False
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest() == expected_sha


def open_verified_archive(run_id: int) -> tuple[dict, str]:
    """返回 (登记行, 磁盘路径)。校验失败先确定性重建自愈合，仍失败则拒绝。"""
    archive = get_archive(run_id)
    if archive is None or archive["status"] != "sealed":
        raise ArchiveError("该批次尚未封存或封存未成功，没有可下载的核账包")

    if not _file_matches(archive["file_path"], archive["file_sha256"]):
        log.warning("下载前校验失败，尝试重建核账包 run=%s", run_id)
        archive = seal_run_archive(run_id)  # 确定性重建，登记哈希随之刷新
        if not _file_matches(archive["file_path"], archive["file_sha256"]):
            raise ArchiveIntegrityError("核账包重建后校验仍失败，拒绝提供下载")
    if not os.path.isfile(archive["file_path"]):
        raise ArchiveIntegrityError("核账包文件在共享卷上不可用")
    return archive, archive["file_path"]
