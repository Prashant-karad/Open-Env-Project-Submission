"""
FastAPI server for the DB Migration Environment.
"""

import sys
import os

_server_dir = os.path.dirname(os.path.abspath(__file__))
_root_dir = os.path.dirname(_server_dir)
sys.path.insert(0, _root_dir)    # for models.py, tasks.py
sys.path.insert(0, _server_dir)  # for db_migration_environment.py

from openenv.core.env_server import create_fastapi_app
from models import MigrationAction, MigrationObservation
from db_migration_environment import DBMigrationEnvironment

app = create_fastapi_app(DBMigrationEnvironment, MigrationAction, MigrationObservation)