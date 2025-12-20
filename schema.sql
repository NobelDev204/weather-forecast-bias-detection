-- Weather Forecast Bias Detection Database Schema
-- Supports multi-city spatial ensemble framework with temporal tracking

-- Forecasts table: stores weather predictions with spatial and temporal metadata
CREATE TABLE IF NOT EXISTS forecasts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Spatial identifiers
    city TEXT NOT NULL,
    grid_id TEXT NOT NULL,
    grid_x INTEGER NOT NULL,
    grid_y INTEGER NOT NULL,
    
    -- Temporal identifiers
    forecast_time TEXT NOT NULL,      -- When forecast was issued (ISO 8601)
    target_date TEXT NOT NULL,        -- Date being forecasted (YYYY-MM-DD)
    forecast_horizon INTEGER NOT NULL, -- Days ahead (0 = same day)
    
    -- Forecast data
    high_temp REAL,
    low_temp REAL,
    conditions TEXT,
    precipitation_chance INTEGER,
    
    -- Data provenance
    source TEXT NOT NULL,             -- e.g., 'weather_underground', 'noaa'
    collected_at TEXT NOT NULL,       -- Timestamp of data collection
    
    -- Deduplication support
    UNIQUE(city, grid_id, grid_x, grid_y, target_date, forecast_time, source)
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_forecasts_city_target 
    ON forecasts(city, target_date);
    
CREATE INDEX IF NOT EXISTS idx_forecasts_horizon 
    ON forecasts(forecast_horizon);
    
CREATE INDEX IF NOT EXISTS idx_forecasts_grid 
    ON forecasts(grid_id, grid_x, grid_y);
    
CREATE INDEX IF NOT EXISTS idx_forecasts_time 
    ON forecasts(forecast_time);

-- Actuals table: stores observed weather for validation
CREATE TABLE IF NOT EXISTS actuals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Location identifier
    city TEXT NOT NULL,
    station_id TEXT,                  -- Weather station identifier
    
    -- Temporal identifier
    date TEXT NOT NULL,               -- Observation date (YYYY-MM-DD)
    
    -- Observed data
    high_temp REAL,
    low_temp REAL,
    conditions TEXT,
    precipitation REAL,               -- Actual precipitation amount
    
    -- Data provenance
    source TEXT NOT NULL,             -- e.g., 'noaa_observations', 'wunderground'
    collected_at TEXT NOT NULL,       -- Timestamp of data collection
    
    -- Ensure one record per city/date/source
    UNIQUE(city, date, source)
);

-- Indexes for actuals
CREATE INDEX IF NOT EXISTS idx_actuals_city_date 
    ON actuals(city, date);
    
CREATE INDEX IF NOT EXISTS idx_actuals_station 
    ON actuals(station_id);

-- View: Join forecasts with actuals for bias analysis
CREATE VIEW IF NOT EXISTS forecast_accuracy AS
SELECT 
    f.city,
    f.grid_id,
    f.grid_x,
    f.grid_y,
    f.target_date,
    f.forecast_horizon,
    f.forecast_time,
    f.high_temp as forecast_high,
    f.low_temp as forecast_low,
    a.high_temp as actual_high,
    a.low_temp as actual_low,
    (f.high_temp - a.high_temp) as high_error,
    (f.low_temp - a.low_temp) as low_error,
    f.source as forecast_source,
    a.source as actual_source
FROM forecasts f
INNER JOIN actuals a 
    ON f.city = a.city 
    AND f.target_date = a.date
WHERE f.high_temp IS NOT NULL 
    AND f.low_temp IS NOT NULL
    AND a.high_temp IS NOT NULL 
    AND a.low_temp IS NOT NULL;

-- View: Spatial consensus aggregation (average across gridpoints per city/date)
CREATE VIEW IF NOT EXISTS spatial_consensus AS
SELECT 
    city,
    target_date,
    forecast_horizon,
    source,
    COUNT(DISTINCT grid_id || '-' || grid_x || '-' || grid_y) as grid_count,
    AVG(high_temp) as consensus_high,
    AVG(low_temp) as consensus_low,
    MIN(high_temp) as min_high,
    MAX(high_temp) as max_high,
    MIN(low_temp) as min_low,
    MAX(low_temp) as max_low
FROM forecasts
WHERE high_temp IS NOT NULL 
    AND low_temp IS NOT NULL
GROUP BY city, target_date, forecast_horizon, source;

