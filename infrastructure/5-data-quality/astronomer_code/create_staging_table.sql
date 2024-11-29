 CREATE TABLE IF NOT EXISTS {{var.value.SCHEMA}}.{{var.value.STAGING_TABLE}} (
        date DATE,
        ticker TEXT,
        high REAL,
        low REAL,
        open REAL,
        close REAL,
        volume INTEGER,
        PRIMARY KEY (date, ticker)
    );