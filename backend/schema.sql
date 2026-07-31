-- Rubaih Greeks schema
CREATE TABLE IF NOT EXISTS engine_status (
    id SERIAL PRIMARY KEY,
    status TEXT NOT NULL,
    detail TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS option_trades (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    symbol TEXT,
    product_id TEXT,
    side TEXT,
    size NUMERIC,
    premium NUMERIC,
    underlying TEXT,
    option_type TEXT,
    strike NUMERIC,
    reason TEXT,
    ai_augmented BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS ai_decisions (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    model TEXT,
    action TEXT,
    confidence NUMERIC,
    reasoning TEXT,
    risk_assessment TEXT,
    portfolio_delta NUMERIC
);

CREATE TABLE IF NOT EXISTS risk_events (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    event_type TEXT,
    detail TEXT
);

CREATE TABLE IF NOT EXISTS greeks_snapshots (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    delta NUMERIC,
    gamma NUMERIC,
    vega NUMERIC,
    theta NUMERIC,
    spot_price NUMERIC,
    session_pnl NUMERIC
);
