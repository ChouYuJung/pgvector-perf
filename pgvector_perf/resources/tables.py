from typing import TYPE_CHECKING, Generic

from pgvector_perf.schemas import PointType

if TYPE_CHECKING:
    from sqlalchemy.sql.schema import MetaData

    from pgvector_perf.client import PgvectorPerf


class Tables(Generic[PointType]):

    _client: "PgvectorPerf[PointType]"

    def __init__(self, client: "PgvectorPerf[PointType]"):
        self._client = client

    def touch(self, *args, **kwargs):
        self.create(*args, **kwargs)

    def create(self, *args, **kwargs):
        engine = self._client.engine
        metadata: "MetaData" = self._client.model._sql_model.metadata

        metadata.create_all(engine, checkfirst=True)
