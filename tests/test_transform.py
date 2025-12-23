"""Unit tests for transformation module"""

import pytest
from datetime import date
from pydantic import ValidationError
from models.pydantic_schemas import MeetingModel, TransformedMeeting
from scripts.transform import DataTransformer
from scripts.clean import DataCleaner


def test_meeting_model_valid():
    """Test valid meeting data"""
    data = {
        'meeting_id': 'TEST-001',
        'city': 'Seattle',
        'date': '2023-06-15',
        'transcript': 'This is a test transcript with enough content.',
        'summary': 'This is a test summary.',
        'agenda': ['Item 1', 'Item 2']
    }
    meeting = MeetingModel(**data)
    assert meeting.meeting_id == 'TEST-001'
    assert meeting.city == 'Seattle'


def test_meeting_model_invalid_date():
    """Test invalid date handling"""
    data = {
        'meeting_id': 'TEST-001',
        'city': 'Seattle',
        'date': '2030-01-01',  # Future date
        'transcript': 'Test transcript content here.',
        'summary': 'Test summary.'
    }
    with pytest.raises(ValidationError):
        MeetingModel(**data)


def test_word_count():
    """Test word counting"""
    transformer = DataTransformer()
    text = "Hello world this is a test"
    assert transformer.count_words(text) == 6


def test_duration_estimation():
    """Test duration estimation"""
    transformer = DataTransformer()
    transcript = " ".join(["word"] * 300)  # 300 words
    duration = transformer.estimate_duration(transcript)
    assert duration == 2  # 300 words / 150 words per minute = 2 minutes


def test_clean_text():
    """Test text cleaning"""
    cleaner = DataCleaner()
    text = "  Hello   world  "
    cleaned = cleaner.clean_text(text)
    assert cleaned == "Hello world"


if __name__ == "__main__":
    pytest.main([__file__, '-v'])