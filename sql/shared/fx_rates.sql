USE test_database;
CREATE TABLE IF NOT EXISTS fx_rates (
  currency_code VARCHAR(16) NOT NULL,
  to_usd_rate DECIMAL(18,8) NOT NULL,
  rate_date DATE NOT NULL,
  PRIMARY KEY (currency_code, rate_date)
);