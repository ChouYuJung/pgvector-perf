import pytest
import sqlalchemy.engine.url
from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine.url import URL

from pgvector_perf.config import logger
from pgvector_perf.schemas import PointWithEmbedding
from pgvector_perf.utils import gen_session_id


@pytest.fixture(autouse=True, scope="module")
def pg_url():
    db_name = f"pgvector_perf_{gen_session_id()}"
    _pg_url_str = (
        "postgresql+psycopg2://postgres:pgvector-perf-password@localhost:15432"
        + f"/{db_name}"
    )
    _pg_url = sqlalchemy.engine.url.make_url(_pg_url_str)
    yield _pg_url

    # Clean up
    try:
        engine = create_engine(_pg_url)
        metadata: MetaData = PointWithEmbedding.metadata
        metadata.drop_all(bind=engine)
    except Exception as e:
        logger.exception(e)


def test_client_database_operations(pg_url: URL):
    print(pg_url)
