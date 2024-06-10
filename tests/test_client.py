import pytest
import sqlalchemy.engine.url
from sqlalchemy import MetaData, create_engine
from sqlalchemy import text as sql_text
from sqlalchemy.engine.url import URL

from pgvector_perf.client import PgvectorPerf
from pgvector_perf.config import console, settings
from pgvector_perf.schemas import PointWithEmbedding, PointWithEmbeddingSchema
from pgvector_perf.utils import dummy_embedding, gen_session_id

test_model_name = "pytest_model"

animals = [
    "dog",
    "cat",
    "fish",
    "bird",
    "rabbit",
    "hamster",
    "turtle",
    "lizard",
    "snake",
    "rat",
    "frog",
]


@pytest.fixture(autouse=True, scope="module")
def pg_url():
    db_name = f"pgvector_perf_{gen_session_id()}"
    _pg_url_str = (
        "postgresql+psycopg2://postgres:pgvector-perf-password@localhost:15432"
    )
    _pg_url_with_db_str = f"{_pg_url_str}/{db_name}"
    _pg_url = sqlalchemy.engine.url.make_url(_pg_url_with_db_str)
    console.print(f"\nPytest session database URL: '{_pg_url}'.")
    yield _pg_url

    # Clean up
    try:
        _pg_url_with_postgres_str = f"{_pg_url_str}/postgres"
        engine = create_engine(_pg_url_with_postgres_str, isolation_level="AUTOCOMMIT")
        metadata: MetaData = PointWithEmbedding.metadata
        with engine.connect() as conn:
            metadata.drop_all(bind=conn)
            console.print(f"\nDropped tables in database '{db_name}'.")
            conn.execute(
                sql_text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    + f"WHERE datname = '{db_name}';"
                )
            )
            conn.execute(sql_text(f"DROP DATABASE IF EXISTS {db_name};"))
            console.print(f"\nDropped database '{db_name}'.")
    except Exception as e:
        console.print_exception()
        raise e


def test_client_database_operations(pg_url: URL):
    console.print(f"\nTesting client database operations with URL: '{pg_url}'.")

    client = PgvectorPerf(url=pg_url, echo=True)

    # Create database
    client.databases.touch()
    # Create tables
    client.tables.touch()
    # Create index
    client.index.touch()


def test_client_point_operations(pg_url: URL):
    console.print(f"\nTesting client database operations with URL: '{pg_url}'.")

    client = PgvectorPerf(url=pg_url, echo=True)

    # Create point
    point = client.points.create(
        PointWithEmbeddingSchema.model_validate(
            {
                "text": f"This is a {animals[0]}.",
                "model": test_model_name,
                "embedding": dummy_embedding(settings.vector_dimensions),
            }
        )
    )
    assert point.id is not None and point.model == test_model_name
    # Create batch points
    batch_points = client.points.create_batch(
        [
            PointWithEmbeddingSchema.model_validate(
                {
                    "text": f"This is a {n}.",
                    "model": test_model_name,
                    "embedding": dummy_embedding(settings.vector_dimensions),
                }
            )
            for n in animals[1:]
        ]
    )
    assert len(batch_points) == 10

    # Get point
    point = client.points.retrieve(point.id, not_found_ok=False)
    assert point.id is not None and point.model == test_model_name
    # List points
    points = client.points.list(model=test_model_name)
    assert len(points) > 1 and all(p.id is not None for p in points)

    # Update point
    _text = point.text
    point.text = point.text + " Updated."
    point = client.points.update(point.id, point)
    assert point.id is not None and point.text.endswith("Updated.")
    # Update point by dict
    point = client.points.update(point.id, text=_text)
    assert point.id is not None and point.text == _text
