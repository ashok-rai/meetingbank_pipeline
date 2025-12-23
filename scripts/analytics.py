"""
Analytics module for MeetingBank Pipeline
Generates insights and visualizations
"""
from scripts.config import Config
import logging
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text
from pymongo import MongoClient
from collections import Counter
import json
from typing import Dict, List, Tuple, Union # Include all types you use

#from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AnalyticsEngine:
    """Generates analytics and visualizations"""
    
    def __init__(self):
        self.config = Config()
        self.pg_engine = None
        self.mongo_client = None
        self.mongo_db = None
        
    def connect_databases(self):
        """Connect to both databases"""
        # PostgreSQL
        conn_str = self.config.get_postgres_connection_string()
        self.pg_engine = create_engine(conn_str)
        logger.info("Connected to PostgreSQL")
        
        # MongoDB
        mongo_str = self.config.get_mongodb_connection_string()
        self.mongo_client = MongoClient(mongo_str)
        self.mongo_db = self.mongo_client[self.config.MONGODB_CONFIG['database']]
        logger.info("Connected to MongoDB")
    
    def query_city_statistics(self) -> pd.DataFrame:
        """Query city-wise meeting statistics from PostgreSQL"""
        logger.info("Querying city statistics...")
        
        query = """
        SELECT 
            c.city_name,
            COUNT(m.meeting_id) as meeting_count,
            AVG(m.duration_min) as avg_duration,
            AVG(m.speaker_count) as avg_speakers,
            AVG(m.transcript_word_count) as avg_transcript_length
        FROM meetings m
        JOIN cities c ON m.city_id = c.city_id
        GROUP BY c.city_name
        ORDER BY meeting_count DESC
        """
        
        df = pd.read_sql(query, self.pg_engine)
        logger.info(f"Retrieved statistics for {len(df)} cities")
        return df
    
    def query_temporal_trends(self) -> pd.DataFrame:
        """Query temporal trends from PostgreSQL"""
        logger.info("Querying temporal trends...")
        
        query = """
        SELECT 
            DATE_TRUNC('month', meeting_date) as month,
            COUNT(*) as meeting_count,
            AVG(duration_min) as avg_duration
        FROM meetings
        GROUP BY month
        ORDER BY month
        """
        
        df = pd.read_sql(query, self.pg_engine)
        logger.info(f"Retrieved {len(df)} months of data")
        return df
    
    # --- ADD THIS METHOD to the AnalyticsEngine class ---

    def query_frequency_by_day_of_week(self) -> pd.DataFrame:
        """Query meeting frequency grouped by the day of the week."""
        logger.info("Querying meeting frequency by day of week...")
        
        query = """
        SELECT 
            TO_CHAR(meeting_date, 'Day') as day_of_week_name,
            EXTRACT(DOW FROM meeting_date) as day_of_week_num, -- 0=Sunday, 1=Monday...
            COUNT(*) as meeting_count
        FROM meetings
        GROUP BY day_of_week_name, day_of_week_num
        ORDER BY day_of_week_num
        """
        
        df = pd.read_sql(query, self.pg_engine)
        logger.info(f"Retrieved frequency for {len(df)} days of the week")
        return df

    # Note: You can now delete or rename query_top_agenda_topics
    # If you choose to keep the name but change the function:
    def query_top_agenda_topics(self, limit: int = 10) -> pd.DataFrame:
        # RETURN AN EMPTY DATAFRAME to prevent errors in the pipeline, 
        # or use the new query above instead, and rename it in the pipeline call.
        logger.info("Skipping agenda topics query as data is unavailable.")
        return pd.DataFrame({'topic': [], 'frequency': []})

    # --- ADD THIS METHOD to the AnalyticsEngine class ---

    def query_summary_metrics(self) -> dict:
        """Calculate average summary compression ratio and related metrics."""
        logger.info("Calculating summary compression metrics...")
        
        # Use SQL aggregation for efficiency
        query = """
        SELECT 
            COUNT(meeting_id) as total_summarized_meetings,
            AVG(CAST(summary_word_count AS NUMERIC) / transcript_word_count) as avg_compression_ratio,
            MAX(CAST(summary_word_count AS NUMERIC) / transcript_word_count) as max_ratio,
            MIN(CAST(summary_word_count AS NUMERIC) / transcript_word_count) as min_ratio,
            AVG(summary_word_count) as avg_summary_length
        FROM meetings
        WHERE transcript_word_count > 0 AND summary_word_count > 0
        """
        
        # Read a single row of metrics
        result = pd.read_sql(query, self.pg_engine).iloc[0].to_dict()
        logger.info(f"Retrieved summary metrics: {result}")
        return result

    # --- ADD A HELPER METHOD FOR REUSABLE WORD COUNTING ---
    def _count_words(self, transcripts) -> Counter:
        """Helper to count and filter words from a MongoDB cursor."""
        stop_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
            'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'be',
            'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will',
            'would', 'should', 'could', 'may', 'might', 'can', 'this', 'that',
            'these', 'those', 'i', 'you', 'he', 'she', 'it', 'we', 'they', 
            'we', 'they', 'just', 'get', 'say', 'go', 'know', 'like', 'don'
        } # Added a few common conversational filler words
        
        all_words = []
        for doc in transcripts:
            text = doc.get('transcript', {}).get('full_text', '')
            words = text.lower().split()
            # Filter out stop words and short words
            words = [w.strip('.,!?"\'') for w in words if w.strip('.,!?"\'') not in stop_words and len(w.strip('.,!?"\'')) > 3]
            all_words.extend(words)
        
        return Counter(all_words)


# --- ADD THIS METHOD to the AnalyticsEngine class ---
    def analyze_keywords_by_city(self, top_n: int = 10) -> Dict:
        """Finds top keywords for the city with the highest number of meetings."""
        logger.info("Analyzing top keywords for the busiest city...")
        
        try:
            # 1. Get the busiest city from PostgreSQL
            city_stats_query = "SELECT city_name FROM cities JOIN meetings ON cities.city_id = meetings.city_id GROUP BY city_name ORDER BY COUNT(meeting_id) DESC LIMIT 1"
            busiest_city_df = pd.read_sql(city_stats_query, self.pg_engine)
            if busiest_city_df.empty:
                 return {'city': 'N/A', 'keywords': []}
                 
            busiest_city = busiest_city_df['city_name'].iloc[0]
            logger.info(f"Busiest city identified: {busiest_city}")
            
            # 2. Query MongoDB for transcripts in that city
            transcripts = self.mongo_db.transcripts.find(
                {'city_name': busiest_city}, 
                {'transcript.full_text': 1}
            )
            
            # 3. Perform keyword analysis
            word_counts = self._count_words(transcripts)
            
            return {
                'city': busiest_city,
                'keywords': word_counts.most_common(top_n)
            }
        except Exception as e:
            logger.error(f"Failed to analyze keywords by city: {str(e)}")
            return {'city': 'Error', 'keywords': []}

    def query_top_agenda_topics(self, limit: int = 10) -> pd.DataFrame:
        """Query most common agenda topics"""
        logger.info("Querying top agenda topics...")
        
        # --- DEBUG STEP: CHECK TABLE COUNT ---
        try:
            with self.pg_engine.connect() as conn:
                # Execute a simple count query
                total_count = conn.execute(text("SELECT COUNT(*) FROM agendas")).scalar()
                logger.info(f"DEBUG: Total records found in 'agendas' table: {total_count}")
        except Exception as e:
            logger.error(f"DEBUG: Failed to count records in 'agendas' table: {str(e)}")
            # Assuming 0 if we can't count
            total_count = 0
        # ------------------------------------
        
        query = f"""
        SELECT 
            topic,
            COUNT(*) as frequency
        FROM agendas
        WHERE topic IS NOT NULL AND topic != '' 
        GROUP BY topic
        ORDER BY frequency DESC
        LIMIT {limit}
        """
        
        df = pd.read_sql(query, self.pg_engine)
        logger.info(f"Retrieved top {len(df)} topics")
        
        # --- DEBUG STEP: CHECK QUERY RESULT ---
        if df.empty and total_count > 0:
            logger.warning("WARNING: 'agendas' table has records, but the topic query returned zero results.")
        # ------------------------------------
        
        return df
    
    def analyze_transcript_keywords(self, top_n: int = 20) -> Counter:
        """Analyze most common words in transcripts from MongoDB"""
        logger.info("Analyzing transcript keywords...")
        
        # Get all transcripts
        transcripts = self.mongo_db.transcripts.find({}, {'transcript.full_text': 1})
        
        # Common stop words to exclude
        stop_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
            'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'be',
            'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will',
            'would', 'should', 'could', 'may', 'might', 'can', 'this', 'that',
            'these', 'those', 'i', 'you', 'he', 'she', 'it', 'we', 'they'
        }
        
        all_words = []
        for doc in transcripts:
            text = doc.get('transcript', {}).get('full_text', '')
            words = text.lower().split()
            # Filter out stop words and short words
            words = [w for w in words if w not in stop_words and len(w) > 3]
            all_words.extend(words)
        
        word_counts = Counter(all_words)
        logger.info(f"Analyzed {len(all_words)} total words")
        return word_counts.most_common(top_n)
    
    def generate_visualizations(self, 
                             city_stats: pd.DataFrame, 
                             temporal_trends: pd.DataFrame,
                             day_of_week_freq: pd.DataFrame) -> dict: # <--- Renamed argument
        """Generate all visualizations"""
        logger.info("Generating visualizations...")
        
        self.config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        outputs = {}
        
        # Figure 1: City-wise meeting counts
        plt.figure(figsize=(12, 6))
        plt.bar(city_stats['city_name'], city_stats['meeting_count'], color='steelblue')
        plt.xlabel('City', fontsize=12)
        plt.ylabel('Number of Meetings', fontsize=12)
        plt.title('Meeting Counts by City', fontsize=14, fontweight='bold')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        
        output_path = self.config.RESULTS_DIR / 'city_meeting_counts.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        outputs['city_counts'] = str(output_path)
        logger.info(f"Saved city meeting counts chart to {output_path}")
        
        # Figure 2: Average meeting duration by city
        plt.figure(figsize=(12, 6))
        plt.bar(city_stats['city_name'], city_stats['avg_duration'], color='coral')
        plt.xlabel('City', fontsize=12)
        plt.ylabel('Average Duration (minutes)', fontsize=12)
        plt.title('Average Meeting Duration by City', fontsize=14, fontweight='bold')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        
        output_path = self.config.RESULTS_DIR / 'city_avg_duration.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        outputs['city_duration'] = str(output_path)
        logger.info(f"Saved duration chart to {output_path}")
        
        # Figure 3: Top agenda topics
        if not day_of_week_freq.empty:
            plt.figure(figsize=(10, 6))
            plt.bar(day_of_week_freq['day_of_week_name'].str.strip(), day_of_week_freq['meeting_count'], color='purple')
        
            plt.xlabel('Day of Week', fontsize=12)
            plt.ylabel('Number of Meetings', fontsize=12)
            plt.title('Meeting Frequency by Day of Week', fontsize=14, fontweight='bold')
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            
            output_path = self.config.RESULTS_DIR / 'meeting_frequency_by_day.png'
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            outputs['day_of_week_freq'] = str(output_path) # <--- New key name
            logger.info(f"Saved day of week frequency chart to {output_path}")

        return outputs
    
    # CHANGE THE FUNCTION SIGNATURE (from 4 args to 7 args)
    def generate_html_report(self, 
                         city_stats: pd.DataFrame, 
                         temporal_trends: pd.DataFrame,
                         keywords: list,
                         chart_files: dict,
                         summary_metrics: dict,      # <--- NEW
                         city_keywords: dict,        # <--- NEW
                         day_of_week_freq: pd.DataFrame) -> Path: # <--- NEW
        """Generate HTML analytics report"""
        logger.info("Generating HTML report...")

        # We need to ensure the HTML structure references the NEW chart file name
        day_of_week_chart_file = chart_files.get('day_of_week_freq', 'meeting_frequency_by_day.png') # Use fallback
        
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>MeetingBank Analytics Report</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 40px;
            background: #f5f5f5;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
        }}
        .section {{
            background: white;
            padding: 25px;
            margin-bottom: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{ margin: 0; }}
        h2 {{ color: #667eea; border-bottom: 2px solid #667eea; padding-bottom: 10px; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background: #667eea;
            color: white;
        }}
        img {{
            max-width: 100%;
            height: auto;
            margin: 20px 0;
            border-radius: 8px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .keywords {{
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
        }}
        .keyword-tag {{
            background: #667eea;
            color: white;
            padding: 8px 15px;
            border-radius: 20px;
            font-size: 14px;
        }}
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-top: 15px;
        }}
        .metric-card {{
            background: #e6e9f9;
            padding: 15px;
            border-radius: 6px;
            text-align: center;
        }}
        .metric-card h4 {{
            margin: 0 0 5px 0;
            color: #4a5d89;
        }}
        .metric-card p {{
            margin: 0;
            font-size: 1.5em;
            font-weight: bold;
            color: #3f51b5;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>MeetingBank Analytics Report</h1>
        <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p>Group 2: Data Orchestrators | Automated Meeting Intelligence Pipeline</p>
    </div>
    
    <div class="section">
        <h2>Summary Generation Metrics</h2>
        <div class="metrics-grid">
            <div class="metric-card">
                <h4>Total Summarized Meetings</h4>
                <p>{summary_metrics['total_summarized_meetings']:.0f}</p>
            </div>
            <div class="metric-card">
                <h4>Avg. Compression Ratio</h4>
                <p>{summary_metrics['avg_compression_ratio'] * 100:.1f}%</p>
            </div>
            <div class="metric-card">
                <h4>Avg. Summary Length</h4>
                <p>{summary_metrics['avg_summary_length']:.0f} words</p>
            </div>
            </div>
    </div>

    <div class="section">
        <h2>City-Wise Statistics</h2>
        {city_stats.to_html(index=False, classes='data-table')}
        
        <h3>Meeting Counts by City</h3>
        <img src="city_meeting_counts.png" alt="City Meeting Counts">
        
        <h3>Average Duration by City</h3>
        <img src="city_avg_duration.png" alt="Average Duration">
    </div>
    
    <div class="section">
        <h2>Meeting Frequency by Day of Week</h2>
        <img src="{day_of_week_chart_file}" alt="Meeting Frequency by Day of Week">
        {day_of_week_freq[['day_of_week_name', 'meeting_count']].rename(columns={'day_of_week_name': 'Day', 'meeting_count': 'Count'}).to_html(index=False, classes='data-table')}
    </div>
    
    <div class="section">
        <h2>Top Keywords for Busiest City: {city_keywords['city']}</h2>
        <div class="keywords">
            {''.join([f'<span class="keyword-tag">{word} ({count})</span>' for word, count in city_keywords['keywords']])}
        </div>
    </div>

    <div class="section">
        <h2>Top Global Keywords from All Transcripts</h2>
        <div class="keywords">
            {''.join([f'<span class="keyword-tag">{word} ({count})</span>' for word, count in keywords[:15]])}
        </div>
    </div>
    
    
    <div class="section">
        <h2>Overall Summary</h2>
        <ul>
            <li><strong>Total Cities Analyzed:</strong> {len(city_stats)}</li>
            <li><strong>Total Meetings Analyzed:</strong> {city_stats['meeting_count'].sum()}</li>
            <li><strong>Average Meeting Duration:</strong> {city_stats['avg_duration'].mean():.1f} minutes</li>
            <li><strong>Average Speakers per Meeting:</strong> {city_stats['avg_speakers'].mean():.1f}</li>
        </ul>
    </div>
</body>
</html>
"""
        
        output_path = self.config.RESULTS_DIR / f'analytics_report_{datetime.now().strftime("%Y%m%d")}.html'
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"Saved HTML report to {output_path}")
        return output_path
    
    def analytics_pipeline(self) -> dict:
        """Complete analytics pipeline"""
        logger.info("="*60)
        logger.info("Starting Analytics Pipeline")
        logger.info("="*60)
        
        try:
            # Connect to databases
            self.connect_databases()
            
            # Run queries
            city_stats = self.query_city_statistics()
            temporal_trends = self.query_temporal_trends()
            top_topics = self.query_top_agenda_topics()
            keywords = self.analyze_transcript_keywords()
            
            # Run queries - NEW
            day_of_week_freq = self.query_frequency_by_day_of_week() # <--- NEW
            summary_metrics = self.query_summary_metrics() # <--- NEW
            city_keywords = self.analyze_keywords_by_city() # <--- NEW

            # Generate visualizations
            # --- PASS ALL NEW DATA TO generate_visualizations ---
            chart_files = self.generate_visualizations(
                city_stats, 
                temporal_trends, 
                #top_topics, 
                day_of_week_freq # <--- NEW
            )
            
            # Generate HTML report
            # --- PASS ALL NEW DATA TO generate_html_report ---
            report_file = self.generate_html_report(
                city_stats, 
                temporal_trends, 
                keywords, 
                chart_files,
                summary_metrics, # <--- NEW
                city_keywords,   # <--- NEW
                day_of_week_freq # <--- NEW
            )
            
            logger.info("="*60)
            logger.info("Analytics Pipeline Completed")
            logger.info(f"Report: {report_file}")
            logger.info("="*60)
            
            return {
                'success': True,
                'report_file': str(report_file),
                'chart_files': chart_files
            }
            
        except Exception as e:
            logger.error(f"Analytics pipeline failed: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
        finally:
            if self.pg_engine:
                self.pg_engine.dispose()
            if self.mongo_client:
                self.mongo_client.close()
