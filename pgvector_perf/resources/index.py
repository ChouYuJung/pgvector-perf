from typing import TYPE_CHECKING

from sqlalchemy import Index as SqlIndex

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
        column_name = "embedding"
        index_name = f"index_{column_name}"

        with engine.connect() as connection:
            index = SqlIndex(
                index_name,
                self._client.model._sql_model.embedding,
                postgresql_using="hnsw",
                postgresql_with={"m": 16, "ef_construction": 64},
                postgresql_ops={"embedding": "vector_l2_ops"},
            )
            index.create(connection, checkfirst=True)
