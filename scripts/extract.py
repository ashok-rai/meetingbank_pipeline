"""
Data extraction module for MeetingBank Pipeline
Fetches meeting data from HuggingFace API
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
import requests
from datasets import load_dataset

from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MeetingBankExtractor:
    """Extracts meeting data from HuggingFace API"""
    
    def __init__(self):
        self.config = Config()
        self.config.create_directories()
        
    def fetch_dataset_from_huggingface(self, subset_size: int = None) -> List[Dict]:
        """
        Fetch MeetingBank dataset from HuggingFace
        
        Args:
            subset_size: Number of meetings to fetch (default from config)
            
        Returns:
            List of meeting dictionaries
        """
        if subset_size is None:
            subset_size = self.config.SUBSET_SIZE
            
        logger.info(f"Fetching {subset_size} meetings from HuggingFace...")
        
        try:
            # Load dataset
            dataset = load_dataset(
                self.config.HUGGINGFACE_DATASET,
                split=f"train[:{subset_size}]",
                token=self.config.HUGGINGFACE_TOKEN
            )
            
            # Convert to list of dictionaries
            meetings = []
            for item in dataset:
                meeting = {
                    'meeting_id': item.get('id', ''),
                    'city': item.get('city', ''),
                    'date': item.get('date', ''),
                    'title': item.get('title', ''),
                    'transcript': item.get('transcript', ''),
                    'summary': item.get('summary', ''),
                    'agenda': item.get('agenda', []),
                    'metadata': {
                        'url': item.get('url', ''),
                        'video_url': item.get('video_url', ''),
                        'source': 'HuggingFace'
                    }
                }
                meetings.append(meeting)
            
            logger.info(f"Successfully fetched {len(meetings)} meetings")
            return meetings
            
        except Exception as e:
            logger.error(f"Failed to fetch dataset: {str(e)}")
            raise
    
    def filter_by_cities(self, meetings: List[Dict]) -> List[Dict]:
        """
        Filter meetings by target cities
        
        Args:
            meetings: List of all meetings
            
        Returns:
            Filtered list of meetings
        """
        logger.info(f"Filtering meetings for cities: {self.config.TARGET_CITIES}")
        
        filtered = [
            m for m in meetings 
            if m.get('city') in self.config.TARGET_CITIES
        ]
        
        logger.info(f"Filtered to {len(filtered)} meetings from target cities")
        return filtered
    
    def save_raw_data(self, meetings: List[Dict], filename: str = None) -> Path:
        """
        Save raw meeting data to JSON file
        
        Args:
            meetings: List of meeting dictionaries
            filename: Custom filename (optional)
            
        Returns:
            Path to saved file
        """
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"meetings_{timestamp}.json"
        
        output_path = self.config.RAW_DATA_DIR / filename
        
        logger.info(f"Saving {len(meetings)} meetings to {output_path}")
        
        data = {
            'metadata': {
                'fetch_date': datetime.now().isoformat(),
                'count': len(meetings),
                'source': self.config.HUGGINGFACE_DATASET
            },
            'meetings': meetings
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Successfully saved data to {output_path}")
        return output_path
    
    def retry_with_backoff(self, func, max_retries: int = None):
        """
        Retry function with exponential backoff
        
        Args:
            func: Function to retry
            max_retries: Maximum retry attempts
            
        Returns:
            Function result
        """
        if max_retries is None:
            max_retries = self.config.API_RETRY_COUNT
        
        for attempt in range(max_retries):
            try:
                return func()
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed after {max_retries} attempts: {str(e)}")
                    raise
                
                wait_time = self.config.API_RETRY_DELAY * (2 ** attempt)
                logger.warning(f"Attempt {attempt + 1} failed. Retrying in {wait_time}s...")
                time.sleep(wait_time)
    
    def extract_pipeline(self, subset_size: int = None) -> Dict:
        """
        Complete extraction pipeline
        
        Args:
            subset_size: Number of meetings to fetch
            
        Returns:
            Dictionary with extraction results
        """
        logger.info("="*60)
        logger.info("Starting MeetingBank Data Extraction Pipeline")
        logger.info("="*60)
        
        start_time = time.time()
        
        try:
            # Step 1: Fetch data with retry logic
            meetings = self.retry_with_backoff(
                lambda: self.fetch_dataset_from_huggingface(subset_size)
            )
            
            # Step 2: Filter by cities
            filtered_meetings = self.filter_by_cities(meetings)
            
            # Step 3: Save raw data
            output_path = self.save_raw_data(filtered_meetings)
            
            # Calculate statistics
            duration = time.time() - start_time
            
            result = {
                'success': True,
                'total_fetched': len(meetings),
                'filtered_count': len(filtered_meetings),
                'output_file': str(output_path),
                'duration_seconds': round(duration, 2),
                'cities': list(set(m['city'] for m in filtered_meetings))
            }
            
            logger.info("="*60)
            logger.info("Extraction Pipeline Completed Successfully")
            logger.info(f"Total fetched: {result['total_fetched']}")
            logger.info(f"Filtered count: {result['filtered_count']}")
            logger.info(f"Duration: {result['duration_seconds']}s")
            logger.info("="*60)
            
            return result
            
        except Exception as e:
            logger.error(f"Extraction pipeline failed: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'duration_seconds': time.time() - start_time
            }


def main():
    """Main function for standalone execution"""
    extractor = MeetingBankExtractor()
    result = extractor.extract_pipeline()
    
    if result['success']:
        print(f"\n✓ Extraction completed successfully!")
        print(f"  Output file: {result['output_file']}")
        print(f"  Meetings extracted: {result['filtered_count']}")
    else:
        print(f"\n✗ Extraction failed: {result.get('error')}")
        exit(1)


if __name__ == "__main__":
    main()