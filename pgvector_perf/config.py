import logging
from typing import Text

from pydantic_settings import BaseSettings
from rich.console import Console

console = Console()


class Settings(BaseSettings):
    logger_name: Text = "pgvector_perf"


settings = Settings()

logger = logging.getLogger(settings.logger_name)
