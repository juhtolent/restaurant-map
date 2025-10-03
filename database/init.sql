CREATE TABLE IF NOT EXISTS restaurants (
    id BIGSERIAL PRIMARY KEY,
    google_id VARCHAR(100) UNIQUE NOT NULL,
    google_name VARCHAR(255) NOT NULL,
    google_display_name VARCHAR(255),
    street_type VARCHAR(50),
    street_name VARCHAR(255),
    street_number VARCHAR(20),
    street_complement VARCHAR(255),
    postalcode VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    business_status VARCHAR(50),
    editorial_summary TEXT,
    google_url VARCHAR(500),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    ratings DECIMAL(3, 2),
    vegetarian_food BOOLEAN DEFAULT FALSE,
    website VARCHAR(500),
    opening_hours_description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Opening_hours table
CREATE TABLE IF NOT EXISTS opening_hours (
    id BIGSERIAL PRIMARY KEY,
    restaurant_id BIGINT NOT NULL,
    day_of_week VARCHAR(25),
    opens_at TIME,
    closes_at TIME,
    is_closed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (restaurant_id) REFERENCES restaurants(id) ON DELETE CASCADE
);

-- Restaurant types (google's classification) table
CREATE TABLE IF NOT EXISTS restaurant_types (
    id BIGSERIAL PRIMARY KEY,
    restaurant_id BIGINT NOT NULL,
    restaurant_type VARCHAR(100) NOT NULL,
    is_primary_type BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (restaurant_id) REFERENCES restaurants(id) ON DELETE CASCADE,
    UNIQUE (restaurant_id, restaurant_type)
);

-- Google API quota management table
CREATE TABLE IF NOT EXISTS google_api_quota (
    id BIGSERIAL PRIMARY KEY,
    month_year DATE NOT NULL,
    api_service VARCHAR(100) NOT NULL, -- "Essential", "Pro", "Enterprise"
    quota_limit INT NOT NULL DEFAULT 500, -- Lower than real quotas 
    quota_used INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
