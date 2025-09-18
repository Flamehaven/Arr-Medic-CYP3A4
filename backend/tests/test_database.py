"""Tests for database timestamp handling."""

from dataclasses import dataclass

import pytest

from backend.database import Database


@pytest.fixture
def anyio_backend():
    """Run the test using asyncio to match the application's runtime."""

    return "asyncio"


@dataclass
class _DummyRequest:
    smiles: str
    compound_id: str


def _sample_result() -> dict:
    return {
        "prediction": "inhibitor",
        "probability": 0.75,
        "confidence": 0.8,
        "risk_level": "medium",
        "descriptors": {"feature": 1.0},
        "warnings": [],
        "processing_time": 0.1,
    }


@pytest.mark.anyio
async def test_recent_24h_counts_new_prediction(tmp_path):
    """Storing a prediction should update the 24h stats bucket."""

    db_path = tmp_path / "test.db"
    database = Database(str(db_path))

    await database.initialize()

    initial_stats = await database.get_stats()
    assert initial_stats["recent_24h"] == 0

    request = _DummyRequest(smiles="CCO", compound_id="test_compound")
    await database.store_prediction(request, _sample_result())

    updated_stats = await database.get_stats()

    assert updated_stats["recent_24h"] == initial_stats["recent_24h"] + 1
    assert updated_stats["total_predictions"] == initial_stats["total_predictions"] + 1
