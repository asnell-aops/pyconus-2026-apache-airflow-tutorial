CREATE TABLE IF NOT EXISTS books (
    isbn    TEXT PRIMARY KEY,
    title   TEXT NOT NULL,
    author  TEXT,
    genre   TEXT,
    price   NUMERIC(6,2)
);

CREATE TABLE IF NOT EXISTS raw_sales (
    sale_id   SERIAL PRIMARY KEY,
    isbn      TEXT,
    sale_date DATE NOT NULL,
    quantity  INT NOT NULL,
    total     NUMERIC(8,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS daily_sales (
    sale_id   SERIAL PRIMARY KEY,
    isbn      TEXT,
    sale_date DATE NOT NULL,
    quantity  INT NOT NULL,
    total     NUMERIC(8,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS sales_quarantine (
    raw            JSONB,
    reason         TEXT,
    quarantined_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS daily_report (
    report_date DATE NOT NULL,
    genre       TEXT NOT NULL,
    books_sold  INT NOT NULL,
    revenue     NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (report_date, genre)
);
