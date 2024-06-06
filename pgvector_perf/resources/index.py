from typing import TYPE_CHECKING, Generic

from sqlalchemy import Index as SqlIndex

from pgvector_perf.schemas import PointType

if TYPE_CHECKING:

    from pgvector_perf.client import PgvectorPerf


class Index(Generic[PointType]):

    _client: "PgvectorPerf[PointType]"

    def __init__(self, client: "PgvectorPerf[PointType]"):
        self._client = client

    def touch(self, *args, **kwargs):
        self.create(*args, **kwargs)

    def create(self, *args, **kwargs):
        engine = self._client.engine

        with engine.connect() as connection:
            index = SqlIndex(
                self._client.vector_index,
                self._client.model._sql_model.embedding,
                postgresql_using="hnsw",
                postgresql_with={"m": 16, "ef_construction": 64},
                postgresql_ops={"embedding": "vector_l2_ops"},
            )
            index.create(connection, checkfirst=True)
