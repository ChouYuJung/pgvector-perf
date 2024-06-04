from typing import TYPE_CHECKING

from sqlalchemy import text as sql_text

if TYPE_CHECKING:
    from pgvector_perf.client import PgvectorPerf


class Databases:

    _client: "PgvectorPerf"

    def __init__(self, client: "PgvectorPerf"):
        self._client = client

    def touch(self, *args, **kwargs):
        engine = self._client.engine
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
                    connection.execute(f"CREATE DATABASE {database_name}")
                    print(f"Database '{database_name}' created successfully.")
                except ProgrammingError as e:
                    print(f"Error creating database: {e}")
            else:
                print(f"Database '{database_name}' already exists.")
