CREATE TABLE IF NOT EXISTS holdings_btc (
    ref_date    TEXT    NOT NULL,
    week        TEXT    NOT NULL,
    day         TEXT    NOT NULL,
    ARKB        REAL,
    BITB        REAL,
    BRRR        REAL,
    BTCO        REAL,
    EZBC        REAL,
    FBTC        REAL,
    GBTC        REAL,
    HODL        REAL,
    IBIT        REAL,
    PRIMARY KEY (ref_date)
)