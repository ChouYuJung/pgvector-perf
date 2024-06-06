import logging
from typing import Optional, Text

from pydantic_settings import BaseSettings
from rich.console import Console

console = Console()


class Settings(BaseSettings):
    logger_name: Text = "pgvector_perf"

    # Program settings
    vector_dimensions: int = 1536
    vector_table: Text = "point_with_embeddings"
    vector_index: Text = "index_embedding"

    # Extra database settings
    admin_database: Text = "postgres"

    # Testing settings
    PYTEST_CURRENT_TEST: Optional[Text] = None


settings = Settings()

logger = logging.getLogger(settings.logger_name)
