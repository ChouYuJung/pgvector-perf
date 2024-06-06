from typing import TYPE_CHECKING, Generic, List, Literal, Optional, Text, overload

from sqlalchemy import select

from pgvector_perf.exceptions import PointNotFoundError
from pgvector_perf.schemas import (
    PointType,
    PointWithEmbedding,
    PointWithEmbeddingSchema,
)

if TYPE_CHECKING:
    from pgvector_perf.client import PgvectorPerf


class Points(Generic[PointType]):

    _client: "PgvectorPerf[PointType]"

    def __init__(self, client: "PgvectorPerf[PointType]"):
        self._client = client

    def list(
        self,
        *args,
        text: Optional[Text],
        model: Optional[Text],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs,
    ) -> List["PointWithEmbeddingSchema"]:
        sql_model = self._client.model.sql_model()

        with self._client.session_factory() as session:
            stmt = select(sql_model)
            if model is not None:
                stmt = stmt.where(sql_model.model == model)
            if text is not None:
                stmt.where(sql_model.text.ilike(f"%{text}%"))
            if limit is not None:
                stmt = stmt.limit(limit)
            if offset is not None:
                stmt = stmt.offset(offset)
            result = session.execute(stmt).scalars().all()
            return [self._client.model.from_sql(point) for point in result]

    @overload
    def retrieve(
        self, id: int, *args, not_found_ok: Literal[False] = False, **kwargs
    ) -> "PointWithEmbeddingSchema": ...

    @overload
    def retrieve(
        self, id: int, *args, not_found_ok: Literal[True], **kwargs
    ) -> Optional["PointWithEmbeddingSchema"]: ...

    def retrieve(
        self, id: int, *args, not_found_ok: bool = False, **kwargs
    ) -> Optional["PointWithEmbeddingSchema"]:
        if not id:
            raise ValueError("No ID provided")

        sql_model: "PointWithEmbedding" = self._client.model._sql_model
        with self._client.session_factory() as session:
            point: Optional["PointWithEmbedding"] = session.query(sql_model).get(
                {"id": id}
            )
            if point is None:
                if not_found_ok:
                    return None
                raise PointNotFoundError(f"No point found with ID: {id}")
            else:
                return PointWithEmbeddingSchema.from_sql(point)

    def create(self, point: PointWithEmbeddingSchema, *args, **kwargs):
        pass

    def update(self, id: Text, point: PointWithEmbeddingSchema, *args, **kwargs):
        pass

    def delete(self, id: Text, *args, **kwargs):
        pass
