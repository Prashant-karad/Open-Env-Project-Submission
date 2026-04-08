"""
Core environment logic for DB Migration Environment.
"""

import sqlite3
import uuid
from typing import Any, Dict, Optional

from openenv.core.env_server.interfaces import Environment
from openenv.core.env_server.types import State

from models import MigrationAction, MigrationObservation
from tasks import TASKS, _get_schema_dict, _schema_consistent


class DBMigrationEnvironment(Environment):

    def __init__(self):
        self._task_id = "task1_add_column"
        self._task_def = TASKS[self._task_id]
        self._migration_log = []
        self._step_number = 0
        self._state = State(episode_id=str(uuid.uuid4()), step_count=0)
        # Always initialize connection immediately — never leave as None
        self._conn = self._fresh_db(self._task_def["ddl"])

    def _fresh_db(self, ddl: str) -> sqlite3.Connection:
        """Create a fresh in-memory SQLite DB from DDL."""
        conn = sqlite3.connect(":memory:", check_same_thread=False)
        conn.execute("PRAGMA foreign_keys = ON")
        for stmt in ddl.split(";"):
            s = stmt.strip()
            if s:
                conn.execute(s)
        conn.commit()
        return conn

    def reset(self, task_id: Optional[str] = None) -> MigrationObservation:
        if task_id and task_id in TASKS:
            self._task_id = task_id
        else:
            self._task_id = "task1_add_column"

        self._task_def = TASKS[self._task_id]
        self._migration_log = []
        self._step_number = 0
        self._state = State(episode_id=str(uuid.uuid4()), step_count=0)

        # Close old connection safely and open fresh one
        try:
            self._conn.close()
        except Exception:
            pass
        self._conn = self._fresh_db(self._task_def["ddl"])

        schema = _get_schema_dict(self._conn)
        return MigrationObservation(
            current_schema=schema,
            last_action_result=self._task_def["description"],
            last_action_success=True,
            migration_log=[],
            target_schema=self._task_def["target_schema"],
            step_number=0,
            actions_remaining=self._task_def["max_steps"],
            reward=0.0,
            done=False,
            task_id=self._task_id,
            hint="Start by calling analyse_schema to inspect the current state.",
        )

    def step(self, action: MigrationAction) -> MigrationObservation:
        # Safety net — if conn somehow None, reinitialize
        if self._conn is None:
            self._conn = self._fresh_db(self._task_def["ddl"])

        self._step_number += 1
        self._state.step_count += 1
        max_steps = self._task_def["max_steps"]
        remaining = max(0, max_steps - self._step_number)

        result, success, reward, hint = self._dispatch(action)
        schema = _get_schema_dict(self._conn)
        done = (remaining == 0) or (action.command.lower().strip() == "validate")
        if done:
            final_score, grade_note = self._task_def["grader"](
                self._conn, self._migration_log
            )
            reward = final_score
            hint = f"Episode complete. Final grade: {final_score:.2f} — {grade_note}"

        return MigrationObservation(
            current_schema=schema,
            last_action_result=result,
            last_action_success=success,
            migration_log=list(self._migration_log),
            target_schema=self._task_def["target_schema"],
            step_number=self._step_number,
            actions_remaining=remaining,
            reward=reward,
            done=done,
            task_id=self._task_id,
            hint=hint,
        )

    @property
    def state(self) -> State:
        return self._state

    def close(self):
        try:
            self._conn.close()
        except Exception:
            pass
        self._conn = None

    def _dispatch(self, action: MigrationAction):
        cmd = action.command.lower().strip()
        params = action.parameters or {}

        handlers = {
            "analyse_schema":    self._analyse_schema,
            "find_dependencies": self._find_dependencies,
            "write_migration":   self._write_migration,
            "dry_run":           self._dry_run,
            "execute":           self._execute,
            "rollback":          self._rollback,
            "validate":          self._validate,
            "observe_result":    self._observe_result,
        }

        if cmd not in handlers:
            return (
                f"Unknown command '{cmd}'. Valid: {list(handlers.keys())}",
                False, -0.05,
                f"Use one of: {list(handlers.keys())}",
            )
        return handlers[cmd](params)

    def _analyse_schema(self, params) -> tuple:
        schema = _get_schema_dict(self._conn)
        lines = []
        for table, info in schema.items():
            cols = ", ".join(
                f"{c}({v['type']}{'  PK' if v['pk'] else ''}{'  NOT NULL' if v['notnull'] else ''})"
                for c, v in info["columns"].items()
            )
            fks = "; ".join(
                f"{f['from']} -> {f['table']}.{f['to']}"
                for f in info["foreign_keys"]
            )
            lines.append(f"TABLE {table}: [{cols}]" + (f"  FK: [{fks}]" if fks else ""))
        return "\n".join(lines), True, 0.05, "Schema analysed."

    def _find_dependencies(self, params) -> tuple:
        col_ref = params.get("column", "")
        if "." not in col_ref:
            return "Provide column as 'table.column'", False, -0.02, "Format: table.column"
        ref_table, ref_col = col_ref.split(".", 1)
        schema = _get_schema_dict(self._conn)
        deps = []
        for table, info in schema.items():
            for fk in info["foreign_keys"]:
                if fk["table"].lower() == ref_table.lower() and fk["to"].lower() == ref_col.lower():
                    deps.append(f"{table}.{fk['from']} -> {ref_table}.{ref_col}")
        if deps:
            return f"Dependencies on {col_ref}:\n" + "\n".join(deps), True, 0.1, "Use this to plan migration order."
        return f"No FK references found for {col_ref}", True, 0.05, ""

    def _write_migration(self, params) -> tuple:
        sql = params.get("sql", "").strip()
        if not sql:
            return "Provide SQL in params: {'sql': '...'}", False, -0.02, ""
        return f"Migration ready:\n{sql}", True, 0.02, "Use dry_run or execute."

    def _dry_run(self, params) -> tuple:
        sql = params.get("sql", "").strip()
        if not sql:
            return "Provide SQL in params: {'sql': '...'}", False, -0.02, ""
        try:
            self._conn.execute("SAVEPOINT dry_run_sp")
            self._conn.execute(sql)
            schema_after = _get_schema_dict(self._conn)
            self._conn.execute("ROLLBACK TO SAVEPOINT dry_run_sp")
            self._conn.execute("RELEASE SAVEPOINT dry_run_sp")
            return f"DRY RUN OK. Schema after:\n{schema_after}", True, 0.03, "Safe to execute."
        except sqlite3.Error as e:
            try:
                self._conn.execute("ROLLBACK TO SAVEPOINT dry_run_sp")
                self._conn.execute("RELEASE SAVEPOINT dry_run_sp")
            except Exception:
                pass
            return f"DRY RUN ERROR: {e}", False, -0.02, "Fix SQL before executing."

    def _execute(self, params) -> tuple:
        sql = params.get("sql", "").strip()
        if not sql:
            return "Provide SQL in params: {'sql': '...'}", False, -0.02, ""
        try:
            self._conn.execute("PRAGMA foreign_keys = ON")
            self._conn.execute(sql)
            self._conn.commit()
            self._migration_log.append(sql)
            return f"EXECUTED OK:\n{sql}", True, 0.2, "Call validate or observe_result next."
        except sqlite3.Error as e:
            self._migration_log.append(f"FAILED: {sql} — {e}")
            return f"EXECUTION ERROR: {e}", False, -0.1, "Read the error and fix the SQL."

    def _rollback(self, params) -> tuple:
        try:
            self._conn.execute("ROLLBACK")
            self._migration_log.append("ROLLBACK")
            return "Rolled back last transaction.", True, 0.05, "Check schema state."
        except sqlite3.Error as e:
            return f"Rollback failed: {e}. Reverse manually with DROP/ALTER.", False, -0.02, ""

    def _validate(self, params) -> tuple:
        consistent, msg = _schema_consistent(self._conn)
        if consistent:
            score, note = self._task_def["grader"](self._conn, self._migration_log)
            return f"VALID. Score estimate: {score:.2f} — {note}", True, 0.05, ""
        return f"INVALID — {msg}", False, -0.05, "Fix constraint violations."

    def _observe_result(self, params) -> tuple:
        schema = _get_schema_dict(self._conn)
        return f"Current schema:\n{schema}", True, 0.0, ""