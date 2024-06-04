from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pgvector_perf.client import PgvectorPerf


class Databases:

    _client: "PgvectorPerf"

    def __init__(self, client: "PgvectorPerf"):
        self._client = client
