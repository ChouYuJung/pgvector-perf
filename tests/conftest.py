import pytest


@pytest.fixture(autouse=True, scope="session")
def check_test_environment():
    from pgvector_perf.config import settings

    pytest_current_test = settings.PYTEST_CURRENT_TEST
    if pytest_current_test is None:
        pytest.exit(
            "PYTEST_CURRENT_TEST is not found in the environment, "
            + "please make sure test environment is set up correctly"
        )
