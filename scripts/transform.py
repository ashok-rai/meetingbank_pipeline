"""
Data transformation module
Derives features and prepares data for storage
"""
from scripts.config import Config
import json
import logging
import pandas as pd
from pathlib import Path
from typing import Dict, List
from datetime import datetime

#from config import Config
from models.pydantic_schemas import TransformedMeeting

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataTransformer:
    """Transforms cleaned data for database loading"""
    
    def __init__(self):
        self.config = Config()
        
    def load_cleaned_data(self, filepath: Path) -> List[Dict]:
        """Load cleaned data"""
        logger.info(f"Loading cleaned data from {filepath}")
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data.get('meetings', [])
    
    def count_words(self, text: str) -> int:
        """Count words in text"""
        if not text:
            return 0
        return len(text.split())
    
    def estimate_duration(self, transcript: str) -> int:
        """Estimate meeting duration from transcript word count"""
        word_count = self.count_words(transcript)
        # Assume average speaking rate: 150 words per minute
        duration = word_count / 150
        return max(1, int(duration))
    
    def count_speakers(self, transcript: str) -> int:
        """Estimate speaker count from transcript"""
        # Simple heuristic: count patterns like "Speaker:", "Name:", etc.
        import re
        speaker_patterns = re.findall(r'\b[A-Z][a-z]+\s*[A-Z]*[a-z]*:', transcript)
        unique_speakers = set(speaker_patterns)
        return max(1, len(unique_speakers))
    
    def transform_meeting(self, meeting: Dict) -> TransformedMeeting:
        """Transform a single meeting record"""
        transcript = meeting.get('transcript', '')
        summary = meeting.get('summary', '')
        
        transformed = TransformedMeeting(
            meeting_id=meeting['meeting_id'],
            city_name=meeting['city'],
            meeting_date=meeting['date'],
            title=meeting.get('title'),
            duration_min=self.estimate_duration(transcript),
            speaker_count=self.count_speakers(transcript),
            transcript_word_count=self.count_words(transcript),
            summary_word_count=self.count_words(summary)
        )
        
        return transformed
    
    def create_structured_data(self, meetings: List[Dict]) -> pd.DataFrame:
        """Create structured data for PostgreSQL"""
        logger.info("Creating structured data...")
        
        structured_records = []
        for meeting in meetings:
            transformed = self.transform_meeting(meeting)
            structured_records.append(transformed.model_dump())
        
        df = pd.DataFrame(structured_records)
        logger.info(f"Created structured DataFrame with {len(df)} records")
        
        return df
    
    def create_dimension_tables(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Create dimension tables"""
        logger.info("Creating dimension tables...")
        
        # Cities dimension
        cities_df = df[['city_name']].drop_duplicates().reset_index(drop=True)
        cities_df['city_id'] = range(1, len(cities_df) + 1)
        cities_df['state'] = cities_df['city_name'].map({
            'Seattle': 'Washington',
            'Boston': 'Massachusetts',
            'Denver': 'Colorado',
            'King County': 'Washington',
            'Long Beach': 'California',
            'Alameda': 'California'
        })
        
        return {'cities': cities_df}
    
    def create_unstructured_data(self, meetings: List[Dict]) -> List[Dict]:
        """Create unstructured data for MongoDB"""
        logger.info("Creating unstructured data...")
        
        unstructured = []
        total_agendas = 0
        
        for meeting in meetings:
            # IMPORTANT: Preserve the agenda field!
            agenda = meeting.get('agenda', [])
            
            # Count for logging
            if agenda:
                total_agendas += len(agenda)
                logger.debug(f"Meeting {meeting['meeting_id']}: {len(agenda)} agenda items")
            
            doc = {
                'meeting_id': meeting['meeting_id'],
                'city_name': meeting['city'],
                'meeting_date': meeting['date'],
                'transcript': {
                    'full_text': meeting.get('transcript', ''),
                    'word_count': self.count_words(meeting.get('transcript', ''))
                },
                'summary': {
                    'full': meeting.get('summary', ''),
                    'short': meeting.get('summary', '')[:200],
                    'word_count': self.count_words(meeting.get('summary', ''))
                },
                'agenda': agenda,  # ← CRITICAL: Keep original agenda!
                'metadata': meeting.get('metadata', {})
            }
            unstructured.append(doc)
        
        logger.info(f"Created {len(unstructured)} unstructured documents")
        logger.info(f"✅ Total agenda items preserved: {total_agendas}")  # ← Add this log!
        
        return unstructured
    
    def transform_pipeline(self, input_file: Path) -> Dict:
        """Complete transformation pipeline"""
        logger.info("="*60)
        logger.info("Starting Data Transformation Pipeline")
        logger.info("="*60)
        
        # Load cleaned data
        meetings = self.load_cleaned_data(input_file)
        logger.info(f"Loaded {len(meetings)} cleaned meetings")
        
        # Create structured data
        structured_df = self.create_structured_data(meetings)
        dimension_tables = self.create_dimension_tables(structured_df)
        
        # Create unstructured data
        unstructured_data = self.create_unstructured_data(meetings)
        
        # Save outputs
        structured_file = self._save_structured_data(structured_df)
        cities_file = self._save_cities_data(dimension_tables['cities'])
        unstructured_file = self._save_unstructured_data(unstructured_data)
        
        logger.info("="*60)
        logger.info("Transformation Pipeline Completed")
        logger.info(f"Structured records: {len(structured_df)}")
        logger.info(f"Cities: {len(dimension_tables['cities'])}")
        logger.info(f"Unstructured documents: {len(unstructured_data)}")
        logger.info("="*60)
        
        return {
            'success': True,
            'structured_file': str(structured_file),
            'cities_file': str(cities_file),
            'unstructured_file': str(unstructured_file),
            'record_count': len(structured_df)
        }
    
    def _save_structured_data(self, df: pd.DataFrame) -> Path:
        """Save structured data to CSV"""
        output_path = self.config.PROCESSED_DATA_DIR / f"structured_data_{datetime.now().strftime('%Y%m%d')}.csv"
        df.to_csv(output_path, index=False)
        logger.info(f"Saved structured data to {output_path}")
        return output_path
    
    def _save_cities_data(self, df: pd.DataFrame) -> Path:
        """Save cities dimension to CSV"""
        output_path = self.config.PROCESSED_DATA_DIR / "cities.csv"
        df.to_csv(output_path, index=False)
        logger.info(f"Saved cities data to {output_path}")
        return output_path
    
    def _save_unstructured_data(self, data: List[Dict]) -> Path:
        """Save unstructured data to JSON"""
        output_path = self.config.PROCESSED_DATA_DIR / f"unstructured_data_{datetime.now().strftime('%Y%m%d')}.json"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved unstructured data to {output_path}")
        return output_path

