"""Resource dùng chung cho asset graph (task 0.12 mục 4)."""

from dagster_project.resources.llm import LLMResource
from dagster_project.resources.notifier import NotifierResource
from dagster_project.resources.postgres import PostgresResource

__all__ = ["LLMResource", "NotifierResource", "PostgresResource"]
