"""
Unit tests for extraction module
"""

import pytest
import json
from pathlib import Path
from scripts.extract import MeetingBankExtractor
from scripts.config import Config


@pytest.fixture
def extractor():
    """Fixture for MeetingBankExtractor instance"""
    return MeetingBankExtractor()


@pytest.fixture
def sample_meetings():
    """Fixture for sample meeting data"""
    return [
        {
            'meeting_id': 'TEST-001',
            'city': 'Seattle',
            'date': '2023-06-15',
            'title': 'City Council Meeting',
            'transcript': 'Test transcript content...',
            'summary': 'Test summary...',
            'agenda': ['Budget', 'Infrastructure'],
            'metadata': {'url': 'http://example.com'}
        },
        {
            'meeting_id': 'TEST-002',
            'city': 'Boston',
            'date': '2023-07-20',
            'title': 'Special Session',
            'transcript': 'Another test transcript...',
            'summary': 'Another summary...',
            'agenda': ['Housing', 'Education'],
            'metadata': {'url': 'http://example.com/2'}
        },
        {
            'meeting_id': 'TEST-003',
            'city': 'Unknown City',  # Should be filtered out
            'date': '2023-08-10',
            'title': 'Test Meeting',
            'transcript': 'Test...',
            'summary': 'Test...',
            'agenda': [],
            'metadata': {}
        }
    ]


def test_config_initialization():
    """Test configuration initialization"""
    config = Config()
    assert config.SUBSET_SIZE == 50
    assert len(config.TARGET_CITIES) == 6
    assert 'Seattle' in config.TARGET_CITIES


def test_create_directories(extractor):
    """Test directory creation"""
    extractor.config.create_directories()
    assert extractor.config.RAW_DATA_DIR.exists()
    assert extractor.config.CLEANED_DATA_DIR.exists()


def test_filter_by_cities(extractor, sample_meetings):
    """Test city filtering"""
    filtered = extractor.filter_by_cities(sample_meetings)
    assert len(filtered) == 2
    assert all(m['city'] in extractor.config.TARGET_CITIES for m in filtered)


def test_save_raw_data(extractor, sample_meetings, tmp_path):
    """Test saving raw data"""
    # Use temporary directory
    extractor.config.RAW_DATA_DIR = tmp_path
    
    output_path = extractor.save_raw_data(sample_meetings, 'test_data.json')
    
    assert output_path.exists()
    
    with open(output_path, 'r') as f:
        data = json.load(f)
    
    assert 'metadata' in data
    assert 'meetings' in data
    assert data['metadata']['count'] == len(sample_meetings)
    assert len(data['meetings']) == len(sample_meetings)


def test_postgres_connection_string():
    """Test PostgreSQL connection string generation"""
    conn_str = Config.get_postgres_connection_string()
    assert 'postgresql://' in conn_str
    assert '@localhost' in conn_str


def test_mongodb_connection_string():
    """Test MongoDB connection string generation"""
    conn_str = Config.get_mongodb_connection_string()
    assert 'mongodb://' in conn_str
    assert '@localhost' in conn_str


if __name__ == "__main__":
    pytest.main([__file__, '-v'])