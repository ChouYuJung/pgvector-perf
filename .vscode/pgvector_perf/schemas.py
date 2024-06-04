from typing import List, Text

from pgvector.sqlalchemy import Vector
from pydantic import BaseModel, ConfigDict
from sqlalchemy import Integer, String
from sqlalchemy.orm import declarative_base, mapped_column

Base = declarative_base()


class TextWithEmbedding(Base):
    __tablename__ = "text_with_embeddings"

    id = mapped_column(Integer, primary_key=True)
    text = mapped_column(String, index=True, nullable=False)
    model = mapped_column(String, nullable=False)
    embedding = mapped_column(Vector(3), nullable=False)


class TextWithEmbeddingSchema(BaseModel):
    model_config: ConfigDict = ConfigDict(from_attributes=True)

    id: int
    text: Text
    model: Text
    embedding: List[float]

    @classmethod
    def from_sql(cls, text_with_embedding: TextWithEmbedding):
        return cls.model_validate(text_with_embedding)

    def to_sql(self) -> TextWithEmbedding:
        return TextWithEmbedding(
            id=self.id,
            text=self.text,
            model=self.model,
            embedding=self.embedding,
        )
