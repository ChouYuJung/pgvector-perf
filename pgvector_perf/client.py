import os
from typing import Optional, Text

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from pgvector_perf import resources
from pgvector_perf.schemas import NOT_GIVEN, NotGiven, PointWithEmbeddingSchema


class PgvectorPerf:

    databases: resources.Databases
    tables: resources.Tables
    index: resources.Index
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
    def database_name(self) -> Text:
        if self._engine.url.database is None:
            raise ValueError("No database name provided in the URL.")
        return self._engine.url.database

    @property
    def session_factory(self):
        return self._session_factory

    @property
    def model(self):
        return self._model
