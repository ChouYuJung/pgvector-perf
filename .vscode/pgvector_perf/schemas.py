from pgvector.sqlalchemy import Vector
from sqlalchemy import Integer, String
from sqlalchemy.orm import declarative_base, mapped_column

Base = declarative_base()


class TextWithEmbedding(Base):
    __tablename__ = "text_with_embeddings"

    id = mapped_column(Integer, primary_key=True)
    text = mapped_column(String, index=True, nullable=False)
    model = mapped_column(String, nullable=False)
    embedding = mapped_column(Vector(3), nullable=False)
