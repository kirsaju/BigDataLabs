-- 02_load_raw.sql
-- Load all 10 CSV files into raw_data. Files are mounted at /data/ in the container.
-- CSV has multiline quoted fields so we use FORMAT csv with QUOTE option.

COPY raw_data FROM '/data/MOCK_DATA.csv'      WITH (FORMAT csv, HEADER true, QUOTE '"', ESCAPE '"');
COPY raw_data FROM '/data/MOCK_DATA (1).csv'   WITH (FORMAT csv, HEADER true, QUOTE '"', ESCAPE '"');
COPY raw_data FROM '/data/MOCK_DATA (2).csv'   WITH (FORMAT csv, HEADER true, QUOTE '"', ESCAPE '"');
COPY raw_data FROM '/data/MOCK_DATA (3).csv'   WITH (FORMAT csv, HEADER true, QUOTE '"', ESCAPE '"');
COPY raw_data FROM '/data/MOCK_DATA (4).csv'   WITH (FORMAT csv, HEADER true, QUOTE '"', ESCAPE '"');
COPY raw_data FROM '/data/MOCK_DATA (5).csv'   WITH (FORMAT csv, HEADER true, QUOTE '"', ESCAPE '"');
COPY raw_data FROM '/data/MOCK_DATA (6).csv'   WITH (FORMAT csv, HEADER true, QUOTE '"', ESCAPE '"');
COPY raw_data FROM '/data/MOCK_DATA (7).csv'   WITH (FORMAT csv, HEADER true, QUOTE '"', ESCAPE '"');
COPY raw_data FROM '/data/MOCK_DATA (8).csv'   WITH (FORMAT csv, HEADER true, QUOTE '"', ESCAPE '"');
COPY raw_data FROM '/data/MOCK_DATA (9).csv'   WITH (FORMAT csv, HEADER true, QUOTE '"', ESCAPE '"');
