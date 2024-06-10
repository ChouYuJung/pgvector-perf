from datetime import datetime
from typing import ClassVar, List, Optional, Text, Type, TypeVar

import pytz
from pgvector.sqlalchemy import Vector
from pydantic import BaseModel, ConfigDict, Field
from rich.style import Style
from rich.text import Text as RichText
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

    id: Optional[int] = None
    text: Text
    model: Text
    embedding: List[float]
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(tz=pytz.utc).replace(microsecond=0)
    )

    _sql_model: ClassVar[Type[PointWithEmbedding]] = PointWithEmbedding

    def __str__(self) -> Text:
        return (
            f"{self.__class__.__name__}("
            + f"id={self.id}, text='{self.text[:20]}', model='{self.model}', "
            + f"embedding=Vector({len(self.embedding)}), "
            + f"created_at='{self.created_at.isoformat()}')"
        )

    def __repr__(self) -> Text:
        return self.__str__()

    def __rich__(self):
        content = RichText("")
        comma = RichText(", ", style="default")
        eq = RichText("=", style="default")
        content += RichText(
            f"{self.__class__.__name__}(",
            style=Style(color="magenta"),
        )
        content += RichText("(", style="default")
        content += RichText("id", style="yellow") + eq
        content += RichText(f"{self.id}", style="cyan") + comma
        content += RichText("text", style="yellow") + eq
        content += RichText(f"'{self.text[:20]}'", style="green") + comma
        content += RichText("model", style="yellow") + eq
        content += RichText(f"'{self.model}'", style="green") + comma
        content += RichText("embedding", style="yellow") + eq
        content += RichText("Vector", style="magenta")
        content += RichText("(", style="default")
        content += RichText(f"{len(self.embedding)}", style="cyan")
        content += RichText(")", style="default") + comma
        content += RichText("created_at", style="yellow") + eq
        content += RichText(f"'{self.created_at.isoformat()}'", style="green")
        content += RichText(")", style="default")
        return content

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
            created_at=self.created_at,
        )

    def update_from_sql(self, point_with_embedding: PointWithEmbedding):
        self.id = point_with_embedding.id
        self.text = point_with_embedding.text
        self.model = point_with_embedding.model
        self.embedding = point_with_embedding.embedding
        self.created_at = point_with_embedding.created_at
        return self


PointType = TypeVar("PointType", bound=PointWithEmbeddingSchema)

NOT_GIVEN = NotGiven()
