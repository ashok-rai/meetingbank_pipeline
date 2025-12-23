-- PostgreSQL Table Definitions
-- MeetingBank Pipeline Database Schema

-- Drop tables if they exist (for fresh start)
DROP TABLE IF EXISTS agendas;
DROP TABLE IF EXISTS meetings;
DROP TABLE IF EXISTS cities;

-- Cities dimension table
CREATE TABLE cities (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL UNIQUE,
    state VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Meetings fact table
CREATE TABLE meetings (
    meeting_id VARCHAR(255) PRIMARY KEY,
    city_id INTEGER NOT NULL REFERENCES cities(city_id),
    meeting_date DATE NOT NULL,
    title TEXT,
    duration_min INTEGER,
    speaker_count INTEGER,
    transcript_word_count INTEGER,
    summary_word_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Agendas detail table
CREATE TABLE agendas (
    agenda_id SERIAL PRIMARY KEY,
    meeting_id VARCHAR(100) NOT NULL REFERENCES meetings(meeting_id),
    item_number INTEGER,
    topic VARCHAR(500),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Comments for documentation
COMMENT ON TABLE cities IS 'Dimension table storing city information';
COMMENT ON TABLE meetings IS 'Fact table storing meeting metadata';
COMMENT ON TABLE agendas IS 'Detail table storing agenda items for meetings';
