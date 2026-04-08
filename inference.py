"""
inference.py — DB Migration Environment agent runner.

OUTPUT FORMAT (strict):
  [START] task=<name> env=db-migration-env model=<model>
  [STEP]  step=<n> action=<str> reward=<0.00> done=<true|false> error=<msg|null>
  [END]   success=<true|false> steps=<n> rewards=<r1,r2,...>
"""

import os
import json
import sys
import time
import requests
from openai import OpenAI

# ── Environment variables ────────────────────────────────────────────────────
API_BASE_URL = "https://api.groq.com/openai/v1"
MODEL_NAME   = "llama-3.3-70b-versatile" 
ENV_URL      = "https://pra-sh-ant-db-migration-env.hf.space"

client = OpenAI(base_url=API_BASE_URL, api_key=GROQ_API_KEY)

# ── Env HTTP helpers ─────────────────────────────────────────────────────────

def env_reset(task_id: str) -> dict:
    r = requests.post(f"{ENV_URL}/reset", json={"task_id": task_id}, timeout=30)
    r.raise_for_status()
    return r.json()

def env_step(command: str, parameters: dict) -> dict:
    payload = {"action": {"command": command, "parameters": parameters}}
    r = requests.post(f"{ENV_URL}/step", json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

def env_close():
    try:
        requests.post(f"{ENV_URL}/close", timeout=10)
    except Exception:
        pass

# ── LLM agent ────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are an expert database migration agent. Respond ONLY with valid JSON.

Valid commands:
  analyse_schema      - inspect tables/columns/constraints
  find_dependencies   - find FK refs to a column
  write_migration     - plan a SQL statement
  dry_run             - preview SQL without committing
  execute             - run SQL against the real DB
  rollback            - undo last executed statement
  validate            - check schema consistency + score (REQUIRED TO FINISH)
  observe_result      - re-read current schema

STRICT RULES:
1. Format: {"command": "...", "parameters": {...}}
2. Do not explain. Do not use markdown.
3. If the migration was successful (the column exists or is renamed), your FINAL ACTION must be "validate".
4. After you call "validate", the environment will set "done": true. 
5. Do not repeat commands that have already succeeded.
"""

def get_agent_action(history: list, obs: dict) -> tuple[str, dict]:
    obs_text = json.dumps(obs, indent=2)
    history.append({"role": "user", "content": f"Current observation:\n{obs_text}\n\nWhat is your next action? Respond with JSON only."})

    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[{"role": "system", "content": SYSTEM_PROMPT}] + history,
        max_tokens=300,
        temperature=0.1, # Lowered temperature for better JSON consistency
        response_format={"type": "json_object"}
    )

    raw = response.choices[0].message.content.strip()
    history.append({"role": "assistant", "content": raw})

    try:
        action = json.loads(raw)
        command = action.get("command", "observe_result")
        parameters = action.get("parameters", {})
    except:
        command = "observe_result"
        parameters = {}

    return command, parameters

# ── Episode runner ────────────────────────────────────────────────────────────

def run_episode(task_id: str) -> dict:
    rewards = []
    steps = 0
    success = False
    history = []

    try:
        obs = env_reset(task_id)
        print(f"[START] task={task_id} env=db-migration-env model={MODEL_NAME}", flush=True)

        done = obs.get("done", False)

        while not done and steps < 10:
            command, parameters = get_agent_action(history, obs)
            action_str = json.dumps({"command": command, "parameters": parameters}).replace("\n", " ")

            try:
                obs = env_step(command, parameters)
            except Exception as e:
                obs = {"done": True, "reward": 0.0, "last_action_result": str(e), "last_action_success": False}

            steps += 1
            reward = obs.get("reward", 0.0)
            done = obs.get("done", False)
            
            error_msg = "null"
            if not obs.get("last_action_success", True):
                error_msg = str(obs.get("last_action_result", "error"))[:100].replace("\n", " ")

            rewards.append(reward)

            print(
                f"[STEP] step={steps} "
                f"action={action_str} "
                f"reward={reward:.2f} "
                f"done={str(done).lower()} "
                f"error={error_msg}",
                flush=True
            )

            if done:
                success = reward >= 0.5

    except Exception as e:
        print(f"[STEP] step={steps+1} action=error reward=0.00 done=true error={str(e)[:100]}", flush=True)
    finally:
        env_close()

    rewards_str = ",".join(f"{r:.2f}" for r in rewards) if rewards else "0.00"
    print(f"[END] success={str(success).lower()} steps={steps} rewards={rewards_str}", flush=True)
    return {"task_id": task_id, "success": success}

# ── Main ──────────────────────────────────────────────────────────────────────

TASK_IDS = [
    "task1_add_column",
    "task2_rename_column",
    "task3_rollback_recovery"
]

if __name__ == "__main__":
    for task_id in TASK_IDS:
        run_episode(task_id)
        time.sleep(2)