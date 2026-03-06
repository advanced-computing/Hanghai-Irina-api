

CREATE OR REPLACE TABLE hate_crimes AS
SELECT * FROM read_csv('NYPD_Hate_Crimes_20260220.csv');

CREATE TABLE IF NOT EXISTS users (
    username VARCHAR,
    age INTEGER,
    country VARCHAR
);