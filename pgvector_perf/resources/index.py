from textwrap import dedent
from typing import TYPE_CHECKING, Text

from sqlalchemy import text as sql_text

from pgvector_perf.config import logger

if TYPE_CHECKING:

    from pgvector_perf.client import PgvectorPerf


class Index:

    _client: "PgvectorPerf"

    def __init__(self, client: "PgvectorPerf"):
        self._client = client

    def touch(self, *args, **kwargs):
        self.create(*args, **kwargs)

    def create(self, *args, **kwargs):
        engine = self._client.engine
        table_name: Text = self._client.model._sql_model.__tablename__
        column_name = "embedding"
        index_name = f"index_{column_name}"

        with engine.connect() as connection:
            # Check if the index exists
            check_index_query = dedent(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = :index_name
                    AND n.nspname = 'public'
                );
                """
            )

            result = connection.execute(
                sql_text(check_index_query), parameters={"index_name": index_name}
            ).scalar()

            # Create the index if it does not exist
            if not result:
                create_index_query = sql_text(
                    f"""
                    CREATE INDEX {index_name} ON {table_name} ({column_name});
                    """.strip()
                )
                connection.execute(create_index_query)
                logger.info(f"Index '{index_name}' created.")
            else:
                logger.debug(f"Index '{index_name}' already exists.")
