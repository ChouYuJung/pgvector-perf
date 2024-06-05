import logging
from typing import Text

from pydantic_settings import BaseSettings
from rich.console import Console

console = Console()


class Settings(BaseSettings):
    logger_name: Text = "pgvector_perf"

    vector_dimensions: int = 1536


settings = Settings()

logger = logging.getLogger(settings.logger_name)
