-- PostgreSQL Index Definitions
-- Optimizes query performance

-- Cities table indexes
CREATE INDEX IF NOT EXISTS idx_cities_name ON cities(city_name);

-- Meetings table indexes
CREATE INDEX IF NOT EXISTS idx_meetings_date ON meetings(meeting_date);
CREATE INDEX IF NOT EXISTS idx_meetings_city ON meetings(city_id);
CREATE INDEX IF NOT EXISTS idx_meetings_city_date ON meetings(city_id, meeting_date);

-- Agendas table indexes
CREATE INDEX IF NOT EXISTS idx_agendas_meeting ON agendas(meeting_id);