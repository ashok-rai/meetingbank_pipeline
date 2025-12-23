"""
Pydantic models for data validation
Defines schemas for meeting data validation
"""

from datetime import date, datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, validator, field_validator
import re


class MeetingMetadata(BaseModel):
    """Meeting metadata schema"""
    url: Optional[str] = None
    video_url: Optional[str] = None
    source: str = "HuggingFace"
    participants: Optional[int] = None


class MeetingModel(BaseModel):
    """Main meeting data validation model"""
    meeting_id: str = Field(..., min_length=1)
    city: str = Field(..., min_length=1)
    date: str
    title: Optional[str] = None
    transcript: str = Field(..., min_length=10)
    summary: str = Field(..., min_length=10)
    agenda: List[str] = Field(default_factory=list)
    metadata: Optional[MeetingMetadata] = None
    
    @field_validator('date')
    @classmethod
    def validate_date(cls, v):
        """Validate date format and ensure it's not in the future"""
        try:
            # Try parsing various date formats
            date_formats = ['%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y']
            parsed_date = None
            
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(v, fmt).date()
                    break
                except ValueError:
                    continue
            
            if parsed_date is None:
                raise ValueError(f'Invalid date format: {v}')
            
            # Check if date is not in the future
            if parsed_date > date.today():
                raise ValueError('Date cannot be in the future')
            
            # Return standardized format
            return parsed_date.strftime('%Y-%m-%d')
            
        except Exception as e:
            raise ValueError(f'Date validation failed: {str(e)}')
    
    @field_validator('city')
    @classmethod
    def validate_city(cls, v):
        """Validate and clean city name"""
        # Remove extra whitespace
        v = ' '.join(v.split())
        # Capitalize properly
        return v.title()
    
    @field_validator('transcript', 'summary')
    @classmethod
    def validate_text_fields(cls, v):
        """Validate and clean text fields"""
        if not v or len(v.strip()) < 10:
            raise ValueError('Text field must be at least 10 characters')
        return v.strip()


class TransformedMeeting(BaseModel):
    """Schema for transformed meeting data"""
    meeting_id: str
    city_name: str
    meeting_date: str
    title: Optional[str]
    duration_min: Optional[int]
    speaker_count: Optional[int]
    transcript_word_count: int
    summary_word_count: int
