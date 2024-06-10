import os
from typing import Generic, Optional, Text

from sqlalchemy import Engine, create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import Session, sessionmaker

from pgvector_perf import resources
from pgvector_perf.config import settings
from pgvector_perf.schemas import (
    NOT_GIVEN,
    NotGiven,
    PointType,
    PointWithEmbeddingSchema,
    Type,
)


class PgvectorPerf(Generic[PointType]):

    databases: resources.Databases[PointType]
    tables: resources.Tables[PointType]
    index: resources.Index[PointType]
    points: resources.Points[PointType]

    def __init__(
        self,
        url: Optional[Text | URL] = None,
        *args,
        model: Type[PointType] = PointWithEmbeddingSchema,
        vector_dimensions: Optional[int] = None,
        vector_table: Optional[Text] = None,
        vector_index: Optional[Text] = None,
        admin_database: Optional[Text] = None,
        echo: bool = False,
        **kwargs,
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
        if not model or model is NOT_GIVEN or isinstance(model, NotGiven):
            raise ValueError("No model provided")

        self._url = url
        self._engine: Optional["Engine"] = None
        self._session_factory: Optional["sessionmaker[Session]"] = None
        self._model: Type[PointType] = model
        self._vector_dimensions = vector_dimensions or settings.vector_dimensions
        self._vector_table = vector_table or settings.vector_table
        self._vector_index = vector_index or settings.vector_index
        self._admin_database = admin_database or settings.admin_database
        self.echo = echo

        # Initialize resources
        self.databases = resources.Databases(self)
        self.tables = resources.Tables(self)
        self.index = resources.Index(self)
        self.points = resources.Points(self)

    @property
    def engine(self):
        if self._engine is None:
            self._engine = create_engine(self._url, echo=self.echo)
        return self._engine

    @property
    def database_name(self) -> Text:
        if self.engine.url.database is None:
            raise ValueError("No database name provided in the URL.")
        return self.engine.url.database

    @property
    def session_factory(self):
        if self._session_factory is None:
            self._session_factory = sessionmaker(bind=self.engine)
        return self._session_factory

    @property
    def model(self):
        return self._model

    @property
    def vector_dimensions(self) -> int:
        return self._vector_dimensions

    @property
    def vector_table(self) -> Text:
        return self._vector_table

    @property
    def vector_index(self) -> Text:
        return self._vector_index
