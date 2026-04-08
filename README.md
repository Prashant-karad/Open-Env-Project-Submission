---
title: DB Migration Environment
emoji: 🗄️
colorFrom: blue
colorTo: purple
sdk: docker
pinned: false
---

# 🗄️ DB Migration Environment

A stateful **Reinforcement Learning environment** for training agents to safely execute database schema migrations.

## What Makes This Different

Most RL environments are glorified Q&A: agent reads → decides → done.

This environment is **genuinely stateful**:
- Every action changes the real database schema
- A wrong action at step 2 breaks something at step 4  
- The agent must **observe the broken state and recover**
- Recovery path varies every time depending on what broke

This is the core property of a real RL environment — **the loop matters**, not just the answer.

## The Three Tasks

| Task | Difficulty | Expected GPT-4 Score |
|------|-----------|----------------------|
| Add nullable column | Easy | 0.85–0.95 |
| Rename column with FK dependencies | Medium | 0.55–0.75 |
| Rollback partial migration | Hard | 0.30–0.55 |

### Task 1 — Safe Column Addition (Easy)
Add a nullable `bio` column to the `employees` table without breaking existing foreign key relationships. One action required. Clear success/fail grading.

### Task 2 — Column Rename with Dependencies (Medium)  
Rename `users.user_id` to `users.id` when `posts` and `comments` both reference it as a foreign key. Agent must find dependencies, plan order, handle constraint errors, and fix them.

### Task 3 — Rollback a Partial Migration (Hard)
A migration ran 3 of 5 steps and crashed, leaving the DB in an inconsistent state. The `shipments` table exists but is **missing its FK constraint**. Agent must diagnose, then either complete or rollback cleanly. No target schema given — agent must infer intent.

## Actions

| Action | Description | Parameters |
|--------|-------------|------------|
| `analyse_schema` | Inspect all tables, columns, constraints | `{}` |
| `find_dependencies` | Find FK refs to a column | `{"column": "table.col"}` |
| `write_migration` | Plan SQL (no execute) | `{"sql": "..."}` |
| `dry_run` | Preview SQL via savepoint | `{"sql": "..."}` |
| `execute` | Run SQL against live DB | `{"sql": "..."}` |
| `rollback` | Undo last statement | `{}` |
| `validate` | Check consistency + score estimate | `{}` |
| `observe_result` | Re-read current schema | `{}` |

## Reward Design

**Step-level:**
- `+0.05` — analyse_schema / observe
- `+0.10` — find_dependencies (correct)
- `+0.20` — successful execute
- `-0.10` — execute causes constraint error
- `-0.05` — validate failure

**Episode-level (final score):**
- `0.60` — final schema correctness vs target
- `0.20` — schema consistency (no broken constraints)
- `0.10` — efficiency bonus
- `0.10` — correct diagnosis at start

## Why Deterministic Grading

SQLite runs in-memory. Given the same starting schema and same actions, output is always identical. Grader compares two Python dicts — **no LLM judge needed**.

## API Usage

```python
import requests

# Reset with specific task
obs = requests.post("http://your-space.hf.space/reset", json={"task_id": "task1_add_column"}).json()

# Take a step
obs = requests.post("http://your-space.hf.space/step", json={
    "command": "analyse_schema",
    "parameters": {}
}).json()

# Get state
state = requests.get("http://your-space.hf.space/state").json()
```

## Technical Details

- **Backend:** SQLite `:memory:` — zero external dependencies
- **Framework:** FastAPI + OpenEnv
- **Docker image:** ~200MB (Python + FastAPI only)
- **Hardware:** Runs comfortably on 2 vCPU / 8GB RAM
