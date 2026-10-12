"""Shared import setup for the directly imported Databricks skill."""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SKILL_DIR = os.path.join(ROOT, "workspace-review")
sys.path.insert(0, os.path.join(SKILL_DIR, "scripts"))
