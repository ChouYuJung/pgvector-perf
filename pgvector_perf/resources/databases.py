from typing import TYPE_CHECKING, Generic, Optional

from sqlalchemy import Engine, create_engine
from sqlalchemy import text as sql_text
from sqlalchemy.exc import ProgrammingError

from pgvector_perf.config import logger
from pgvector_perf.schemas import PointType

if TYPE_CHECKING:
    from pgvector_perf.client import PgvectorPerf


class Databases(Generic[PointType]):

    _client: "PgvectorPerf[PointType]"

    def __init__(self, client: "PgvectorPerf[PointType]"):
        self._client = client

    def touch(self, *args, **kwargs):
        self.create(*args, exist_ok=True, **kwargs)
        self.activate_vector(*args, **kwargs)

    def create(self, *args, exist_ok: bool = True, **kwargs):
        engine = self._default_engine(auto_commit=True)
        db_name = self._client.database_name

        with engine.connect() as connection:
            # Check if the database exists
            result = connection.execute(
                sql_text(f"SELECT 1 FROM pg_database WHERE datname='{db_name}'")
            )
            exists = result.scalar() is not None

            if not exists:
                # If the database does not exist, create it
                try:
                    connection.execute(sql_text(f"CREATE DATABASE {db_name}"))
                    logger.info(f"Database '{db_name}' created successfully.")
                except ProgrammingError as e:
                    logger.error(f"Error creating database '{db_name}': {e}")
                    raise e
            else:
                msg = f"Database '{db_name}' already exists."
                if exist_ok:
                    logger.debug(msg)
                else:
                    logger.error(msg)
                    raise ValueError(msg)

    def activate_vector(self, *args, **kwargs):
        engine = self._client.engine
        ext_name = "vector"

        # Connect to the database
        with engine.connect() as connection:
            # Check if the extension exists
            result = connection.execute(
                sql_text(f"SELECT 1 FROM pg_extension WHERE extname='{ext_name}'")
            )
            exists = result.scalar() is not None

            if not exists:
                # If the extension does not exist, create it
                try:
                    connection.execute(sql_text(f"CREATE EXTENSION {ext_name}"))
                    connection.commit()
                    logger.info(f"Extension '{ext_name}' created successfully.")
                except ProgrammingError as e:
                    logger.error(f"Error creating extension: {e}")
                    raise e
            else:
                logger.debug(f"Extension '{ext_name}' already exists.")

    def _default_engine(
        self,
        auto_commit: Optional[bool] = None,
        **kwargs,
    ) -> "Engine":
        url = self._client.engine.url.set(database="postgres")
        extra_parameters = kwargs
        if auto_commit is not None:
            extra_parameters["isolation_level"] = "AUTOCOMMIT"
        return create_engine(url, echo=self._client.echo, **extra_parameters)
