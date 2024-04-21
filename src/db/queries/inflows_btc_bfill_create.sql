CREATE TABLE IF NOT EXISTS inflows_btc_bfill (
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
    TOTAL       REAL,
    PRIMARY KEY (ref_date)
)