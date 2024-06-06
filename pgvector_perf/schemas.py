from typing import List, Text, Type

from pgvector.sqlalchemy import Vector
from pydantic import BaseModel, ConfigDict, PrivateAttr
from sqlalchemy import Integer, String
from sqlalchemy.orm import declarative_base, mapped_column

from pgvector_perf.config import settings

Base = declarative_base()


class NotGiven:
    pass


class PointWithEmbedding(Base):
    __tablename__ = "point_with_embeddings"

    id = mapped_column(Integer, primary_key=True)
    text = mapped_column(String, nullable=False)
    model = mapped_column(String, index=True, nullable=False, default="default")
    embedding = mapped_column(Vector(settings.vector_dimensions), nullable=False)


class PointWithEmbeddingSchema(BaseModel):
    model_config: ConfigDict = ConfigDict(from_attributes=True)

    id: int
    text: Text
    model: Text
    embedding: List[float]

    _sql_model: Type[PointWithEmbedding] = PrivateAttr(default=PointWithEmbedding)

    @classmethod
    def sql_model(cls) -> Type[PointWithEmbedding]:
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


NOT_GIVEN = NotGiven()
