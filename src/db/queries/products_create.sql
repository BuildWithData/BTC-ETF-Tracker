CREATE TABLE IF NOT EXISTS products (
    ticker      TEXT    NOT NULL    PRIMARY KEY,
    type        TEXT    NOT NULL    CHECK (type in ('etf', 'etp')),
    firm        TEXT    NOT NULL,
    country     TEXT                CHECK (country in ('USA', 'CANADA'))
)