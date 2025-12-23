"""
Data cleaning module
Handles data quality checks and cleaning operations
"""
from scripts.config import Config
import json
import logging
from pathlib import Path
from typing import List, Dict, Tuple
from datetime import datetime
from pydantic import ValidationError
from typing import Dict, Optional, Tuple
from models.pydantic_schemas import MeetingModel
#from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataCleaner:
    """Handles data cleaning and validation"""
    
    def __init__(self):
        self.config = Config()
        self.validation_errors = []
        
    def load_raw_data(self, filepath: Path) -> Dict:
        """Load raw data from JSON file"""
        logger.info(f"Loading raw data from {filepath}")
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    
    def validate_meeting(self, meeting: Dict) -> Tuple[bool, Optional[MeetingModel], Optional[str]]:
        """
        Validate a single meeting record
        
        Returns:
            Tuple of (is_valid, validated_meeting, error_message)
        """
        try:
            validated = MeetingModel(**meeting)
            return True, validated, None
        except ValidationError as e:
            error_msg = str(e)
            return False, None, error_msg
    
    def remove_duplicates(self, meetings: List[Dict]) -> List[Dict]:
        """Remove duplicate meetings based on meeting_id"""
        seen_ids = set()
        unique_meetings = []
        
        for meeting in meetings:
            meeting_id = meeting.get('meeting_id')
            if meeting_id and meeting_id not in seen_ids:
                seen_ids.add(meeting_id)
                unique_meetings.append(meeting)
        
        duplicates_removed = len(meetings) - len(unique_meetings)
        if duplicates_removed > 0:
            logger.warning(f"Removed {duplicates_removed} duplicate meetings")
        
        return unique_meetings
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""
        
        # Remove extra whitespace
        text = ' '.join(text.split())
        
        # Remove special characters but keep basic punctuation
        # text = re.sub(r'[^\w\s.,!?-]', '', text)
        
        return text.strip()
    
    def standardize_date(self, date_str: str) -> str:
        """Standardize date format to YYYY-MM-DD"""
        date_formats = ['%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y', '%Y/%m/%d']
        
        for fmt in date_formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        return date_str  # Return original if no format matches
    
    def clean_pipeline(self, input_file: Path) -> Dict:
        """
        Complete cleaning pipeline
        
        Returns:
            Dictionary with cleaning results
        """
        logger.info("="*60)
        logger.info("Starting Data Cleaning Pipeline")
        logger.info("="*60)
        
        # Load raw data
        raw_data = self.load_raw_data(input_file)
        meetings = raw_data.get('meetings', [])
        
        logger.info(f"Loaded {len(meetings)} raw meetings")
        
        # Remove duplicates
        meetings = self.remove_duplicates(meetings)
        
        # Validate and clean each meeting
        valid_meetings = []
        invalid_meetings = []
        
        for idx, meeting in enumerate(meetings):
            # Clean text fields
            if 'transcript' in meeting:
                meeting['transcript'] = self.clean_text(meeting['transcript'])
            if 'summary' in meeting:
                meeting['summary'] = self.clean_text(meeting['summary'])
            if 'date' in meeting:
                meeting['date'] = self.standardize_date(meeting['date'])
            
            # Validate
            is_valid, validated, error = self.validate_meeting(meeting)
            
            if is_valid:
                valid_meetings.append(validated.model_dump())
            else:
                invalid_meetings.append({
                    'meeting_id': meeting.get('meeting_id', f'unknown_{idx}'),
                    'error': error
                })
                logger.warning(f"Invalid meeting {meeting.get('meeting_id')}: {error}")
        
        # Generate quality report
        quality_report = self._generate_quality_report(
            len(meetings),
            len(valid_meetings),
            invalid_meetings
        )
        
        # Save cleaned data
        output_file = self._save_cleaned_data(valid_meetings)
        report_file = self._save_quality_report(quality_report)
        
        logger.info("="*60)
        logger.info("Cleaning Pipeline Completed")
        logger.info(f"Valid meetings: {len(valid_meetings)}")
        logger.info(f"Invalid meetings: {len(invalid_meetings)}")
        logger.info(f"Success rate: {quality_report['success_rate']:.1f}%")
        logger.info("="*60)
        
        return {
            'success': True,
            'valid_count': len(valid_meetings),
            'invalid_count': len(invalid_meetings),
            'output_file': str(output_file),
            'report_file': str(report_file),
            'quality_report': quality_report
        }
    
    def _generate_quality_report(self, total: int, valid: int, invalid: List) -> Dict:
        """Generate data quality report"""
        success_rate = (valid / total * 100) if total > 0 else 0
        
        return {
            'timestamp': datetime.now().isoformat(),
            'total_records': total,
            'valid_records': valid,
            'invalid_records': len(invalid),
            'success_rate': success_rate,
            'error_rate': 100 - success_rate,
            'errors': invalid
        }
    
    def _save_cleaned_data(self, meetings: List[Dict]) -> Path:
        """Save cleaned data to JSON file"""
        output_path = self.config.CLEANED_DATA_DIR / f"meetings_cleaned_{datetime.now().strftime('%Y%m%d')}.json"
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump({'meetings': meetings}, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved cleaned data to {output_path}")
        return output_path
    
    def _save_quality_report(self, report: Dict) -> Path:
        """Save quality report"""
        self.config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        report_path = self.config.REPORTS_DIR / f"quality_report_{datetime.now().strftime('%Y%m%d')}.json"
        
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"Saved quality report to {report_path}")
        return report_path