from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    List,
    Literal,
    Optional,
    Sequence,
    Text,
    Tuple,
    overload,
)

from sqlalchemy import select

import pgvector_perf.exceptions
from pgvector_perf.schemas import PointType
from pgvector_perf.utils import batch_process

if TYPE_CHECKING:
    from pgvector_perf.client import PgvectorPerf


class Points(Generic[PointType]):

    _client: "PgvectorPerf[PointType]"

    hard_limit: int = 1000

    def __init__(self, client: "PgvectorPerf[PointType]"):
        self._client = client

    def query(
        self,
        embedding: List[float],
        *args,
        limit: int = 5,
        within_distance: Optional[float] = None,
        **kwargs,
    ) -> List[Tuple[PointType, float]]:
        if len(embedding) != self._client.vector_dimensions:
            raise ValueError(
                f"Embedding must have {self._client.vector_dimensions} dimensions"
            )
        limit = max(1, min(limit, self.hard_limit))
        within_distance = (
            None
            if within_distance is not None and within_distance <= 0
            else within_distance
        )

        sql_model = self._client.model.sql_model()

        with self._client.session_factory() as session:
            stmt = select(
                sql_model,
                sql_model.embedding.l2_distance(embedding).label("distance"),
            )
            if within_distance is not None:
                stmt = stmt.where(
                    sql_model.embedding.l2_distance(embedding) < within_distance
                )
            stmt = stmt.order_by(sql_model.embedding.l2_distance(embedding))
            stmt = stmt.limit(limit)
            result = session.execute(stmt).all()
            return [
                (self._client.model.from_sql(point), distance)
                for point, distance in result
            ]

    def list(
        self,
        *args,
        text: Optional[Text] = None,
        model: Optional[Text] = None,
        limit: int = 5,
        offset: Optional[int] = None,
        sort_desc: bool = True,
        **kwargs,
    ) -> List[PointType]:
        limit = max(1, min(limit, self.hard_limit))

        sql_model = self._client.model.sql_model()

        with self._client.session_factory() as session:
            stmt = select(sql_model)
            if text is not None:
                stmt.where(sql_model.text.ilike(f"%{text}%"))
            if model is not None:
                stmt = stmt.where(sql_model.model == model)
            stmt = stmt.limit(limit)
            if offset is not None:
                stmt = stmt.offset(offset)
            stmt = stmt.order_by(
                sql_model.created_at.desc() if sort_desc else sql_model.created_at.asc()
            )
            result = session.execute(stmt).scalars().all()
            return [self._client.model.from_sql(point) for point in result]

    @overload
    def retrieve(
        self, id: int, *args, not_found_ok: Literal[False] = False, **kwargs
    ) -> PointType: ...

    @overload
    def retrieve(
        self, id: int, *args, not_found_ok: Literal[True], **kwargs
    ) -> Optional[PointType]: ...

    def retrieve(
        self, id: int, *args, not_found_ok: bool = False, **kwargs
    ) -> Optional[PointType]:
        if not id:
            raise ValueError("No ID provided")

        sql_model = self._client.model._sql_model

        with self._client.session_factory() as session:
            stmt = select(sql_model)
            stmt = stmt.where(sql_model.id == id)
            point = session.execute(stmt).scalar_one_or_none()
            if point is None:
                if not_found_ok:
                    return None
                raise pgvector_perf.exceptions.PointNotFoundError(
                    f"No point found with ID: {id}"
                )
            else:
                return self._client.model.from_sql(point)

    def create(self, point: PointType, *args, **kwargs) -> PointType:
        with self._client.session_factory() as session:
            sql_point = self._client.model.to_sql(point)
            session.add(sql_point)
            session.commit()
            session.refresh(sql_point)
            return point.update_from_sql(sql_point)

    def create_batch(
        self, points: Sequence[PointType], *args, batch_size: int = 16, **kwargs
    ) -> List[PointType]:
        output_points: List[PointType] = []
        with self._client.session_factory() as session:
            for points_chunk in batch_process(points, batch_size=batch_size):
                sql_points = [
                    self._client.model.to_sql(point) for point in points_chunk
                ]
                session.add_all(sql_points)
                session.commit()
                for _point, sql_point in zip(points_chunk, sql_points):
                    session.refresh(sql_point)
                    _point.update_from_sql(sql_point)
                    output_points.append(_point)
        return output_points

    def update(
        self,
        id: int,
        point: Optional[PointType] = None,
        *args,
        update_attrs: Optional[Dict[Text, Any]] = None,
        **kwargs,
    ) -> PointType:
        _update_attrs = {} if point is None else point.model_dump(exclude_none=True)
        _update_attrs.update(update_attrs or {})
        _update_attrs.update(kwargs)
        if len(_update_attrs) == 0:
            raise ValueError("No attributes provided to update.")

        # Validate that the ID is provided
        with self._client.session_factory() as session:
            sql_model = self._client.model._sql_model
            stmt = select(sql_model).where(sql_model.id == id)
            sql_point = session.execute(stmt).scalar_one_or_none()
            if sql_point is None:
                raise pgvector_perf.exceptions.PointNotFoundError(
                    f"No point found with ID: {id}"
                )

            for key, value in _update_attrs.items():
                if value is not None:
                    setattr(sql_point, key, value)

            session.commit()
            session.refresh(sql_point)
            return self._client.model.from_sql(sql_point)

    def delete(self, id: int, *args, not_found_ok: bool = False, **kwargs) -> bool:
        with self._client.session_factory() as session:
            sql_model = self._client.model._sql_model
            stmt = select(sql_model).where(sql_model.id == id)
            sql_point = session.execute(stmt).scalar_one_or_none()
            if sql_point is None:
                if not_found_ok:
                    return False
                raise pgvector_perf.exceptions.PointNotFoundError(
                    f"No point found with ID: {id}"
                )

            session.delete(sql_point)
            session.commit()
            return True
