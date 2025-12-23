-- Sample Analytical Queries for MeetingBank Database

-- Query 1: City-wise meeting statistics
SELECT 
    c.city_name,
    c.state,
    COUNT(m.meeting_id) as total_meetings,
    AVG(m.duration_min) as avg_duration_minutes,
    AVG(m.speaker_count) as avg_speakers,
    MIN(m.meeting_date) as first_meeting,
    MAX(m.meeting_date) as last_meeting
FROM cities c
LEFT JOIN meetings m ON c.city_id = m.city_id
GROUP BY c.city_id, c.city_name, c.state
ORDER BY total_meetings DESC;

-- Query 2: Monthly meeting trends
SELECT 
    DATE_TRUNC('month', meeting_date) as month,
    COUNT(*) as meeting_count,
    AVG(duration_min) as avg_duration,
    SUM(transcript_word_count) as total_words
FROM meetings
GROUP BY month
ORDER BY month;

-- Query 3: Longest meetings by city
SELECT 
    c.city_name,
    m.title,
    m.meeting_date,
    m.duration_min,
    m.transcript_word_count
FROM meetings m
JOIN cities c ON m.city_id = c.city_id
ORDER BY m.duration_min DESC
LIMIT 10;

-- Query 4: Most discussed agenda topics
SELECT 
    topic,
    COUNT(*) as frequency,
    COUNT(DISTINCT meeting_id) as meetings_count
FROM agendas
WHERE topic IS NOT NULL
GROUP BY topic
ORDER BY frequency DESC
LIMIT 20;

-- Query 5: Meetings with most speakers
SELECT 
    c.city_name,
    m.title,
    m.meeting_date,
    m.speaker_count,
    m.duration_min
FROM meetings m
JOIN cities c ON m.city_id = c.city_id
ORDER BY m.speaker_count DESC
LIMIT 10;

-- Query 6: Average transcript length by city
SELECT 
    c.city_name,
    AVG(m.transcript_word_count) as avg_transcript_words,
    AVG(m.summary_word_count) as avg_summary_words,
    AVG(m.transcript_word_count::float / NULLIF(m.summary_word_count, 0)) as compression_ratio
FROM meetings m
JOIN cities c ON m.city_id = c.city_id
GROUP BY c.city_name
ORDER BY avg_transcript_words DESC;
