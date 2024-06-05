from typing import TYPE_CHECKING, Text

from pgvector_perf.schemas import PointWithEmbeddingSchema

if TYPE_CHECKING:
    from pgvector_perf.client import PgvectorPerf


class Points:

    _client: "PgvectorPerf"

    def __init__(self, client: "PgvectorPerf"):
        self._client = client

    def retrieve(self, id: Text, *args, **kwargs):
        pass

    def create(self, point: PointWithEmbeddingSchema, *args, **kwargs):
        pass

    def update(self, id: Text, point: PointWithEmbeddingSchema, *args, **kwargs):
        pass

    def delete(self, id: Text, *args, **kwargs):
        pass
