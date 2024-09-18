CREATE TABLE IF NOT EXISTS fact_dk_props (
    subcategory_subcategoryId VARCHAR,
    subcategory_name VARCHAR,
    offer_label VARCHAR,
    offer_providerOfferId VARCHAR,
    offer_eventId VARCHAR,
    offer_eventGroupId VARCHAR,
    offer_playerNameIdentifier VARCHAR,
    outcome_label VARCHAR,
    outcome_oddsAmerican VARCHAR,
    outcome_oddsDecimal DOUBLE,
    outcome_line DOUBLE,
    participant_id VARCHAR,
    participant_name VARCHAR,
    participant_type VARCHAR,
    timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_participant (
    participant_id VARCHAR PRIMARY KEY,
    participant_name VARCHAR,
    participant_type VARCHAR
);

