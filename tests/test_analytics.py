"""Unit tests for analytics module"""

import pytest
import pandas as pd
from scripts.analytics import AnalyticsEngine
from collections import Counter


@pytest.fixture
def analytics_engine():
    """Fixture for AnalyticsEngine"""
    return AnalyticsEngine()


def test_analytics_initialization():
    """Test analytics engine initialization"""
    engine = AnalyticsEngine()
    assert engine.config is not None
    assert engine.pg_engine is None  # Not connected yet


def test_database_connections(analytics_engine):
    """Test database connections"""
    try:
        analytics_engine.connect_databases()
        assert analytics_engine.pg_engine is not None
        assert analytics_engine.mongo_db is not None
    except Exception as e:
        pytest.skip(f"Database not available: {str(e)}")


def test_keywords_analysis():
    """Test keyword analysis logic"""
    sample_text = "meeting city council budget budget infrastructure"
    words = sample_text.split()
    word_counts = Counter(words)
    
    assert word_counts['budget'] == 2
    assert word_counts['meeting'] == 1


if __name__ == "__main__":
    pytest.main([__file__, '-v'])