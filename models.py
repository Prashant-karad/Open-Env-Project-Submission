from typing import Any, Dict, List, Optional
from pydantic import Field
from openenv.core.env_server.types import Action, Observation


class MigrationAction(Action):
    """Action for the DB Migration environment."""
    command: str = Field(..., description=(
        "One of: analyse_schema | find_dependencies | write_migration | "
        "execute | dry_run | rollback | validate | observe_result"
    ))
    parameters: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Command-specific params. "
            "For execute/dry_run/write_migration: {'sql': '<SQL statement>'}. "
            "For find_dependencies: {'column': 'table.column'}. "
            "Others: {}"
        )
    )


class MigrationObservation(Observation):
    """Observation returned by the DB Migration environment."""
    current_schema: Dict[str, Any] = Field(..., description="Current DB schema as dict")
    last_action_result: str = Field(..., description="Result/error from last action")
    last_action_success: bool = Field(..., description="Whether last action succeeded")
    migration_log: List[str] = Field(default_factory=list, description="SQL statements executed so far")
    target_schema: Optional[Dict[str, Any]] = Field(None, description="Target schema (None for task 3)")
    step_number: int = Field(..., description="Current step number")
    actions_remaining: int = Field(..., description="Steps remaining before episode ends")
    reward: float = Field(0.0, description="Reward from last action")
    done: bool = Field(False, description="Whether episode is complete")
    task_id: str = Field(..., description="Current task identifier")
    hint: str = Field("", description="Contextual hint for the agent")
