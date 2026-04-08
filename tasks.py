"""
Task definitions for the DB Migration Environment.
Each task has:
  - initial_schema: SQL DDL to set up the starting database
  - target_schema:  What the schema should look like after success
  - description:    Shown to the agent
  - max_steps
  - grader():       Returns float 0.0-1.0 given final sqlite3 connection
"""

from typing import Any, Dict, Optional
import sqlite3


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_schema_dict(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Introspect live DB → Python dict for easy comparison."""
    schema: Dict[str, Any] = {}
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cur.fetchall()]
    for table in tables:
        cur.execute(f"PRAGMA table_info({table})")
        cols = {}
        for row in cur.fetchall():
            cols[row[1]] = {
                "type": row[2],
                "notnull": bool(row[3]),
                "default": row[4],
                "pk": bool(row[5]),
            }
        cur.execute(f"PRAGMA foreign_key_list({table})")
        fks = []
        for row in cur.fetchall():
            fks.append({"from": row[3], "table": row[2], "to": row[4]})
        schema[table] = {"columns": cols, "foreign_keys": fks}
    return schema


def _schema_consistent(conn: sqlite3.Connection) -> tuple[bool, str]:
    """Check FK consistency (SQLite integrity_check)."""
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_key_check")
    violations = cur.fetchall()
    if violations:
        return False, f"FK violations: {violations}"
    cur.execute("PRAGMA integrity_check")
    result = cur.fetchone()[0]
    if result != "ok":
        return False, f"Integrity check: {result}"
    return True, "ok"


# ─────────────────────────────────────────────────────────────────────────────
# TASK 1 — Easy: Add a nullable column
# ─────────────────────────────────────────────────────────────────────────────

TASK1_DDL = """
PRAGMA foreign_keys = ON;

CREATE TABLE departments (
    id   INTEGER PRIMARY KEY,
    name TEXT    NOT NULL
);

CREATE TABLE employees (
    id            INTEGER PRIMARY KEY,
    name          TEXT    NOT NULL,
    department_id INTEGER REFERENCES departments(id)
);

CREATE TABLE projects (
    id          INTEGER PRIMARY KEY,
    title       TEXT NOT NULL,
    lead_emp_id INTEGER REFERENCES employees(id)
);

INSERT INTO departments VALUES (1,'Engineering'),(2,'Marketing');
INSERT INTO employees   VALUES (1,'Alice',1),(2,'Bob',2);
INSERT INTO projects    VALUES (1,'Alpha',1),(2,'Beta',2);
"""

TASK1_DESCRIPTION = """
You are a database migration agent.

TASK 1 — EASY: Safe Column Addition
====================================
Add a new nullable TEXT column called `bio` to the `employees` table.
The column must be nullable (no NOT NULL constraint) and must not break
any existing foreign key relationships.

Available actions:
  analyse_schema          — inspect current tables/columns/constraints
  write_migration         — produce SQL (params: {"sql": "ALTER TABLE ..."})
  dry_run                 — preview SQL without committing (params: {"sql": "..."})
  execute                 — run SQL against the DB (params: {"sql": "..."})
  validate                — check schema consistency and correctness
  observe_result          — re-read current schema state

You have 15 steps. Good luck.
"""

TASK1_TARGET = {
    "employees_has_bio": True,
    "bio_nullable": True,
}


def grade_task1(conn: sqlite3.Connection, migration_log: list) -> tuple[float, str]:
    consistent, msg = _schema_consistent(conn)
    if not consistent:
        return 0.0, f"Schema inconsistent: {msg}"

    cur = conn.cursor()
    cur.execute("PRAGMA table_info(employees)")
    cols = {row[1]: row for row in cur.fetchall()}

    if "bio" not in cols:
        # Partial credit if they at least analysed correctly
        partial = 0.1 if any("analyse" in s.lower() or "bio" in s.lower() for s in migration_log) else 0.0
        return partial, "Column 'bio' not added to employees table"

    col = cols["bio"]
    notnull = bool(col[3])
    if notnull:
        return 0.5, "Column 'bio' added but has NOT NULL — must be nullable"

    return 1.0, "Perfect: nullable 'bio' column added, schema consistent"


# ─────────────────────────────────────────────────────────────────────────────
# TASK 2 — Medium: Rename column with FK dependencies
# ─────────────────────────────────────────────────────────────────────────────

TASK2_DDL = """
PRAGMA foreign_keys = ON;

CREATE TABLE users (
    user_id   INTEGER PRIMARY KEY,
    username  TEXT NOT NULL,
    email     TEXT NOT NULL
);

CREATE TABLE posts (
    post_id   INTEGER PRIMARY KEY,
    title     TEXT NOT NULL,
    author_id INTEGER NOT NULL REFERENCES users(user_id)
);

CREATE TABLE comments (
    comment_id INTEGER PRIMARY KEY,
    body       TEXT NOT NULL,
    author_id  INTEGER NOT NULL REFERENCES users(user_id),
    post_id    INTEGER NOT NULL REFERENCES posts(post_id)
);

INSERT INTO users    VALUES (1,'alice','a@x.com'),(2,'bob','b@x.com');
INSERT INTO posts    VALUES (1,'Hello',1),(2,'World',2);
INSERT INTO comments VALUES (1,'Nice!',2,1),(2,'Thanks',1,1);
"""

TASK2_DESCRIPTION = """
You are a database migration agent.

TASK 2 — MEDIUM: Column Rename with Dependencies
=================================================
Rename the column `user_id` in the `users` table to `id`.
IMPORTANT: `posts.author_id` and `comments.author_id` both reference
`users.user_id` as a foreign key. You must update ALL references
correctly. Getting the order wrong will produce constraint errors —
read those errors and fix them.

Available actions:
  analyse_schema          — inspect current tables/columns/constraints
  find_dependencies       — find FKs referencing a column (params: {"column": "users.user_id"})
  write_migration         — produce SQL (params: {"sql": "..."})
  dry_run                 — preview SQL without committing (params: {"sql": "..."})
  execute                 — run SQL (params: {"sql": "..."})
  rollback                — undo last execute
  validate                — check schema consistency
  observe_result          — re-read current schema

You have 20 steps. Mistakes give negative reward but you can recover.
"""

TASK2_TARGET = {
    "users_has_id": True,
    "users_no_user_id": True,
    "fks_intact": True,
}


def grade_task2(conn: sqlite3.Connection, migration_log: list) -> tuple[float, str]:
    consistent, msg = _schema_consistent(conn)
    schema = _get_schema_dict(conn)

    score = 0.0
    notes = []

    # Did they find dependencies? (+0.15)
    found_deps = any("find_dep" in s.lower() or "author_id" in s.lower() for s in migration_log)
    if found_deps:
        score += 0.15
        notes.append("✓ Found dependencies")

    # Is users.id present? (+0.35)
    users_cols = schema.get("users", {}).get("columns", {})
    if "id" in users_cols:
        score += 0.35
        notes.append("✓ users.id exists")
    else:
        notes.append("✗ users.id missing")
        return round(score, 2), " | ".join(notes)

    # Is users.user_id gone? (+0.1)
    if "user_id" not in users_cols:
        score += 0.1
        notes.append("✓ users.user_id removed")
    else:
        notes.append("✗ users.user_id still present (rename incomplete)")

    # Schema consistent? (+0.3)
    if consistent:
        score += 0.3
        notes.append("✓ Schema consistent, no FK violations")
    else:
        notes.append(f"✗ {msg}")

    # Data intact? (+0.1)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users")
        if cur.fetchone()[0] == 2:
            score += 0.1
            notes.append("✓ Data intact")
    except Exception:
        notes.append("✗ Data check failed")

    return round(min(score, 1.0), 2), " | ".join(notes)


# ─────────────────────────────────────────────────────────────────────────────
# TASK 3 — Hard: Rollback a partially executed migration
# ─────────────────────────────────────────────────────────────────────────────

# The migration was SUPPOSED to:
#   1. Add `status` column to orders ✓ (done)
#   2. Add `shipped_at` column to orders ✓ (done)
#   3. Create `shipments` table ✓ (done)
#   4. Add FK: shipments.order_id → orders.id  ✗ FAILED (orphaned table)
#   5. Populate shipments from orders            ✗ NOT RUN
#
# So the DB is in a broken intermediate state:
#   - orders has status + shipped_at (fine)
#   - shipments table exists BUT has no FK constraint (broken)
#   - No data in shipments

TASK3_DDL = """
PRAGMA foreign_keys = ON;

CREATE TABLE customers (
    id   INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE orders (
    id          INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    total       REAL    NOT NULL,
    status      TEXT    DEFAULT 'pending',
    shipped_at  TEXT    DEFAULT NULL
);

-- Partially created shipments table (missing FK, broken state)
CREATE TABLE shipments (
    id         INTEGER PRIMARY KEY,
    order_id   INTEGER NOT NULL,
    carrier    TEXT    NOT NULL,
    tracked_at TEXT    DEFAULT NULL
    -- NOTE: FK to orders.id is MISSING — this is the broken state
);

INSERT INTO customers VALUES (1,'Acme Corp'),(2,'Globex');
INSERT INTO orders    VALUES (1,1,250.00,'pending',NULL),(2,2,99.50,'pending',NULL);
"""

TASK3_DESCRIPTION = """
You are a database migration agent.

TASK 3 — HARD: Recover from a Partially Executed Migration
===========================================================
A migration ran partway and left the database in an INCONSISTENT state.

What we know ran (migration log):
  STEP 1: ALTER TABLE orders ADD COLUMN status TEXT DEFAULT 'pending'   ✓
  STEP 2: ALTER TABLE orders ADD COLUMN shipped_at TEXT DEFAULT NULL    ✓
  STEP 3: CREATE TABLE shipments (id INTEGER PRIMARY KEY, order_id INTEGER NOT NULL, carrier TEXT NOT NULL, tracked_at TEXT DEFAULT NULL)  ✓
  STEP 4: (FAILED) — adding FK constraint shipments.order_id → orders.id
  STEP 5: (NOT RUN) — INSERT INTO shipments SELECT ...

The `shipments` table exists but is MISSING its foreign key to `orders`.
This is a broken intermediate state.

YOUR GOAL: Restore a CONSISTENT schema. You may either:
  A) COMPLETE the migration — add the missing FK properly
  B) ROLLBACK — drop the shipments table and revert to clean state

No target schema is given — you must DIAGNOSE the state and decide.

Available actions:
  analyse_schema    — inspect current tables/columns/constraints
  find_dependencies — find references (params: {"column": "table.col"})
  execute           — run SQL (params: {"sql": "..."})
  dry_run           — preview SQL without committing (params: {"sql": "..."})
  rollback          — undo last execute
  validate          — check schema consistency
  observe_result    — re-read current schema

You have 25 steps. Diagnosis accuracy is scored separately from outcome.
"""


def grade_task3(conn: sqlite3.Connection, migration_log: list) -> tuple[float, str]:
    consistent, msg = _schema_consistent(conn)
    schema = _get_schema_dict(conn)
    score = 0.0
    notes = []

    # Did agent diagnose the problem? (+0.2)
    diagnosed = any(
        any(k in s.lower() for k in ["analyse", "observe", "find_dep", "pragma", "shipment"])
        for s in migration_log
    )
    if diagnosed:
        score += 0.2
        notes.append("✓ Diagnosed intermediate state")
    else:
        notes.append("✗ No diagnosis attempted")

    # Final schema consistent? (+0.4)
    if consistent:
        score += 0.4
        notes.append("✓ Final schema is consistent")
    else:
        notes.append(f"✗ Schema inconsistent: {msg}")
        return round(score, 2), " | ".join(notes)

    # Strategy: Complete OR Rollback — both valid
    orders_cols = schema.get("orders", {}).get("columns", {})
    shipments = schema.get("shipments")
    has_status = "status" in orders_cols
    has_shipped_at = "shipped_at" in orders_cols

    if shipments is not None:
        # Strategy A: Complete migration
        fks = shipments.get("foreign_keys", [])
        fk_to_orders = any(f["table"] == "orders" and f["from"] == "order_id" for f in fks)
        if fk_to_orders:
            score += 0.3
            notes.append("✓ Strategy A: FK added correctly (migration completed)")
        else:
            score += 0.05
            notes.append("~ Strategy A attempted but FK still missing")
    else:
        # Strategy B: Rollback — shipments dropped
        if not has_status and not has_shipped_at:
            # Full rollback
            score += 0.3
            notes.append("✓ Strategy B: Full rollback (shipments dropped, orders reverted)")
        elif has_status and has_shipped_at:
            # Partial rollback — only dropped shipments table, left orders columns
            score += 0.25
            notes.append("~ Strategy B: shipments dropped; orders columns kept (acceptable)")
        else:
            score += 0.1
            notes.append("~ Strategy B: partial, inconsistent rollback")

    # Data intact? (+0.1)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM orders")
        if cur.fetchone()[0] == 2:
            score += 0.1
            notes.append("✓ Data intact")
    except Exception:
        notes.append("✗ Data lost")

    return round(min(score, 1.0), 2), " | ".join(notes)


# ─────────────────────────────────────────────────────────────────────────────
# Registry
# ─────────────────────────────────────────────────────────────────────────────

TASKS = {
    "task1_add_column": {
        "ddl": TASK1_DDL,
        "description": TASK1_DESCRIPTION,
        "max_steps": 15,
        "grader": grade_task1,
        "target_schema": TASK1_TARGET,
        "difficulty": "easy",
    },
    "task2_rename_column": {
        "ddl": TASK2_DDL,
        "description": TASK2_DESCRIPTION,
        "max_steps": 20,
        "grader": grade_task2,
        "target_schema": TASK2_TARGET,
        "difficulty": "medium",
    },
    "task3_rollback_recovery": {
        "ddl": TASK3_DDL,
        "description": TASK3_DESCRIPTION,
        "max_steps": 25,
        "grader": grade_task3,
        "target_schema": None,   # intentionally hidden for task 3
        "difficulty": "hard",
    },
}
