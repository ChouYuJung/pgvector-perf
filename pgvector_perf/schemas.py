from datetime import datetime
from typing import ClassVar, List, Text, Type, TypeVar

import pytz
from pgvector.sqlalchemy import Vector
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import DateTime, Integer, String
from sqlalchemy.orm import declarative_base, mapped_column

from pgvector_perf.config import settings

Base = declarative_base()


class NotGiven:
    pass


class PointWithEmbedding(Base):
    __tablename__ = settings.vector_table

    id = mapped_column(Integer, primary_key=True)
    text = mapped_column(String, nullable=False)
    model = mapped_column(String, index=True, nullable=False, default="default")
    embedding = mapped_column(Vector(settings.vector_dimensions), nullable=False)
    created_at = mapped_column(
        DateTime,
        index=True,
        nullable=False,
        default=lambda: datetime.now(tz=pytz.utc),
    )


class PointWithEmbeddingSchema(BaseModel):
    model_config: ConfigDict = ConfigDict(from_attributes=True)

    id: int
    text: Text
    model: Text
    embedding: List[float]
    created_at: datetime = Field(default_factory=lambda: datetime.now(tz=pytz.utc))

    _sql_model: ClassVar[Type[PointWithEmbedding]] = PointWithEmbedding

    @classmethod
    def sql_model(cls):
        return cls._sql_model

    @classmethod
    def from_sql(cls, point_with_embedding: PointWithEmbedding):
        return cls.model_validate(point_with_embedding)

    def to_sql(self) -> PointWithEmbedding:
        return self._sql_model(
            id=self.id,
            text=self.text,
            model=self.model,
            embedding=self.embedding,
        )


PointType = TypeVar("PointType", bound=PointWithEmbeddingSchema)

NOT_GIVEN = NotGiven()
