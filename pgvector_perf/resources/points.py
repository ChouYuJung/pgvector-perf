from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    List,
    Literal,
    Optional,
    Text,
    overload,
)

from sqlalchemy import select

import pgvector_perf.exceptions
from pgvector_perf.schemas import PointType

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
        sort_desc: bool = True,
        **kwargs,
    ) -> List[PointType]:
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
            return self._client.model.from_sql(sql_point)

    def update(
        self,
        id: Text,
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

    def delete(self, id: Text, *args, not_found_ok: bool = False, **kwargs) -> bool:
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
