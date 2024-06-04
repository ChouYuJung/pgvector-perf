import os
from typing import Optional, Text

from pgvector_perf import resources
from pgvector_perf.schemas import NOT_GIVEN, NotGiven, PointWithEmbeddingSchema
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class PgvectorPerf:

    databases: resources.Databases
    tables: resources.Tables
    points: resources.Points

    def __init__(
        self,
        url: Optional[Text] = None,
        model: PointWithEmbeddingSchema | NotGiven = NOT_GIVEN,
        echo: bool = False,
    ):
        # Validate url
        url = (
            url
            or os.environ.get("POSTGRES_URL")
            or os.environ.get("POSTGRESQL_URL")
            or os.environ.get("DATABASE_URL")
        )
        if url is None:
            raise ValueError("No PostgreSQL URL provided")
        # Validate model
        if not isinstance(model, PointWithEmbeddingSchema):
            raise ValueError("Model must be an instance of PointWithEmbeddingSchema.")

        self._engine = create_engine(url, echo=echo)
        self._session_factory = sessionmaker(bind=self._engine)
        self._model = model

        # Initialize resources
        self.databases = resources.Databases(self)
        self.tables = resources.Tables(self)
        self.points = resources.Points(self)

    @property
    def engine(self):
        return self._engine

    @property
    def session_factory(self):
        return self._session_factory

    @property
    def model(self):
        return self._model
