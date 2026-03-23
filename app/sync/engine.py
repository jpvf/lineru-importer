"""Core sync logic: schema, data, views, routines."""
import logging
import re
import threading
import time
import traceback
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

from app.aurora.connection import get_aurora_conn
from app.aurora.discovery import (
    get_table_columns, get_views, get_routines, get_row_count
)
from app.local.connection import get_local_conn, ensure_database_exists
from app.notifications.telegram import notify
from app.state import repository as repo
from app.sync.batch import bulk_replace, bulk_upsert


class SyncCancelledError(Exception):
    pass


class SyncEngine:
    def __init__(self, job_id: int, pause_event: threading.Event, cancel_event: threading.Event):
        self.job_id = job_id
        self.pause_event = pause_event
        self.cancel_event = cancel_event
        self.batch_size = int(repo.effective_settings()["batch_size"])

    # ─── Control ──────────────────────────────────────────────────────────────

    def check_control(self):
        if self.cancel_event.is_set():
            raise SyncCancelledError()
        while self.pause_event.is_set() and not self.cancel_event.is_set():
            time.sleep(0.5)
        if self.cancel_event.is_set():
            raise SyncCancelledError()

    # ─── Entry point ──────────────────────────────────────────────────────────

    def run(self):
        now = datetime.now(timezone.utc).isoformat()
        try:
            ensure_database_exists()
            aurora = get_aurora_conn()

            selected = repo.get_selected_tables()
            all_tables = repo.get_tables()

            repo.update_job(
                self.job_id,
                status="running",
                started_at=now,
                tables_total=len(all_tables),
            )
            notify(f"🚀 *Aurora Sync started* (job #{self.job_id})\n{len(selected)} tables selected for data, {len(all_tables)} for schema.")

            # 1. Schema for ALL tables (fresh local connection per table)
            self._sync_all_schemas(aurora, all_tables)
            self.check_control()

            # 2. Views and routines
            local = get_local_conn()
            self._sync_views_and_routines(aurora, local)
            local.close()
            self.check_control()

            # 3. Data for selected tables — fresh connection per table
            aurora.close()

            total_rows = sum(t.get("row_count_estimate") or 0 for t in selected)
            repo.update_job(self.job_id, rows_total=total_rows)

            rows_done = 0
            for i, table in enumerate(selected):
                self.check_control()
                rows = self._sync_table_data(table)
                rows_done += rows
                repo.update_job(self.job_id, tables_done=i + 1, rows_done=rows_done)

            repo.update_job(
                self.job_id,
                status="done",
                completed_at=datetime.now(timezone.utc).isoformat()
            )
            notify(f"✅ *Aurora Sync complete* (job #{self.job_id})\n{rows_done:,} rows synced.")

        except SyncCancelledError:
            repo.update_job(
                self.job_id,
                status="cancelled",
                cancelled_at=datetime.now(timezone.utc).isoformat()
            )
            notify(f"🛑 *Aurora Sync cancelled* (job #{self.job_id})")

        except Exception as e:
            tb = traceback.format_exc()
            logger.error("Sync job %s failed:\n%s", self.job_id, tb)
            repo.update_job(
                self.job_id,
                status="error",
                error_message=str(e)[:500]
            )
            notify(f"❌ *Aurora Sync ERROR* (job #{self.job_id})\n`{str(e)[:200]}`")

    # ─── Schema ───────────────────────────────────────────────────────────────

    def _sync_all_schemas(self, aurora, tables: list[dict]):
        for table in tables:
            self.check_control()
            name   = table["table_name"]
            schema = table["schema_name"]

            # Skip DROP+CREATE if table already has synced data
            state = repo.get_sync_state(schema, name)
            if state and state.get("status") in ("done", "running", "paused") and (state.get("rows_synced") or 0) > 0:
                continue

            try:
                with aurora.cursor() as cur:
                    cur.execute(f"SHOW CREATE TABLE `{name}`")
                    row = cur.fetchone()
                    ddl = row.get("Create Table", "")

                ddl = re.sub(r'\s+AUTO_INCREMENT=\d+', '', ddl)

                # Fresh local connection per table — avoids idle timeout on long schema runs
                local = get_local_conn()
                try:
                    with local.cursor() as cur:
                        cur.execute("SET SESSION sql_mode='NO_ENGINE_SUBSTITUTION'")
                        cur.execute("SET FOREIGN_KEY_CHECKS=0")
                        cur.execute(f"DROP TABLE IF EXISTS `{name}`")
                        cur.execute(ddl)
                        cur.execute("SET FOREIGN_KEY_CHECKS=1")
                    local.commit()
                finally:
                    local.close()

            except Exception as e:
                repo.upsert_sync_state(
                    schema, name,
                    status="error", error_message=f"schema: {e}"
                )

    # ─── Views & Routines ─────────────────────────────────────────────────────

    def _sync_views_and_routines(self, aurora, local):
        # Views
        for view in get_views(aurora):
            self.check_control()
            try:
                defn = view["definition"]
                defn = re.sub(r'CREATE\s+.*?VIEW', 'CREATE OR REPLACE VIEW', defn, flags=re.IGNORECASE)
                with local.cursor() as cur:
                    cur.execute(defn)
                local.commit()
            except Exception:
                pass

        # Stored procedures and functions
        for routine in get_routines(aurora):
            self.check_control()
            try:
                with local.cursor() as cur:
                    rtype = routine["type"]
                    rname = routine["name"]
                    cur.execute(f"DROP {rtype} IF EXISTS `{rname}`")
                    cur.execute(routine["definition"])
                local.commit()
            except Exception:
                pass

    # ─── Data ─────────────────────────────────────────────────────────────────

    def _sync_table_data(self, table: dict) -> int:
        name     = table["table_name"]
        schema   = table["schema_name"]
        strategy = table["cursor_strategy"]

        aurora = get_aurora_conn()
        local  = get_local_conn()
        # Allow zero dates and other Aurora quirks in the local MySQL session
        with local.cursor() as cur:
            cur.execute("SET SESSION sql_mode='NO_ENGINE_SUBSTITUTION'")
        local.commit()

        try:
            insertable_cols, _ = get_table_columns(aurora, name)
            if not insertable_cols:
                return 0

            total  = get_row_count(aurora, name)
            log_id = repo.create_job_table_log(self.job_id, schema, name, total)
            repo.upsert_sync_state(schema, name, status="running", total_rows_at_start=total)
            repo.update_job_table_log(
                log_id, status="running",
                started_at=datetime.now(timezone.utc).isoformat(),
                rows_total=total
            )

            if strategy == "auto_increment":
                rows_done = self._sync_auto_increment(aurora, local, name, schema, insertable_cols, log_id)
            elif strategy == "datetime":
                rows_done = self._sync_datetime(aurora, local, name, schema, insertable_cols, log_id)
            else:
                rows_done = self._sync_full_offset(aurora, local, name, schema, insertable_cols, log_id)

            repo.upsert_sync_state(schema, name, status="done", rows_synced=rows_done)
            repo.update_job_table_log(
                log_id, status="done",
                rows_synced=rows_done,
                completed_at=datetime.now(timezone.utc).isoformat()
            )
            return rows_done

        except SyncCancelledError:
            repo.upsert_sync_state(schema, name, status="paused")
            repo.update_job_table_log(log_id, status="paused")
            raise

        except Exception as e:
            repo.upsert_sync_state(schema, name, status="error", error_message=str(e)[:300])
            repo.update_job_table_log(log_id, status="error", error_message=str(e)[:300])
            return 0

        finally:
            try:
                aurora.close()
            except Exception:
                pass

    def _sync_auto_increment(self, aurora, local, name, schema, cols, log_id) -> int:
        pk_col    = repo.get_table(schema, name)["auto_increment_col"]
        state     = repo.get_sync_state(schema, name)
        last_id   = (state or {}).get("last_synced_id") or 0
        col_list  = ", ".join(f"`{c}`" for c in cols)
        rows_done = (state or {}).get("rows_synced") or 0

        while True:
            self.check_control()
            with aurora.cursor() as cur:
                cur.execute(
                    f"SELECT {col_list} FROM `{name}` WHERE `{pk_col}` > %s ORDER BY `{pk_col}` ASC LIMIT %s",
                    (last_id, self.batch_size)
                )
                batch = cur.fetchall()

            if not batch:
                break

            bulk_upsert(local, name, cols, batch)
            last_id    = batch[-1][pk_col]
            rows_done += len(batch)
            repo.upsert_sync_state(schema, name, last_synced_id=last_id, rows_synced=rows_done)
            repo.update_job_table_log(log_id, rows_synced=rows_done)

        return rows_done

    def _sync_datetime(self, aurora, local, name, schema, cols, log_id) -> int:
        dt_col    = repo.get_table(schema, name)["updated_at_col"]
        state     = repo.get_sync_state(schema, name)
        last_dt   = (state or {}).get("last_synced_at") or "1970-01-01 00:00:00"
        col_list  = ", ".join(f"`{c}`" for c in cols)
        rows_done = (state or {}).get("rows_synced") or 0

        while True:
            self.check_control()
            with aurora.cursor() as cur:
                cur.execute(
                    f"SELECT {col_list} FROM `{name}` WHERE `{dt_col}` >= %s ORDER BY `{dt_col}` ASC LIMIT %s",
                    (last_dt, self.batch_size)
                )
                batch = cur.fetchall()

            if not batch:
                break

            bulk_upsert(local, name, cols, batch)
            val = batch[-1][dt_col]
            last_dt = val.isoformat() if hasattr(val, "isoformat") else str(val)
            rows_done += len(batch)
            repo.upsert_sync_state(schema, name, last_synced_at=last_dt, rows_synced=rows_done)
            repo.update_job_table_log(log_id, rows_synced=rows_done)

            if len(batch) < self.batch_size:
                break

        return rows_done

    def _sync_full_offset(self, aurora, local, name, schema, cols, log_id) -> int:
        state     = repo.get_sync_state(schema, name)
        offset    = (state or {}).get("last_offset") or 0
        col_list  = ", ".join(f"`{c}`" for c in cols)
        rows_done = offset

        if offset == 0:
            with local.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE `{name}`")
            local.commit()

        while True:
            self.check_control()
            with aurora.cursor() as cur:
                cur.execute(
                    f"SELECT {col_list} FROM `{name}` LIMIT %s OFFSET %s",
                    (self.batch_size, offset)
                )
                batch = cur.fetchall()

            if not batch:
                repo.upsert_sync_state(schema, name, last_offset=0)
                break

            bulk_replace(local, name, cols, batch)
            offset    += len(batch)
            rows_done  = offset
            repo.upsert_sync_state(schema, name, last_offset=offset, rows_synced=rows_done)
            repo.update_job_table_log(log_id, rows_synced=rows_done)

        return rows_done
