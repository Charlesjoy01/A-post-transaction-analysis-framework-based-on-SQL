USE test_database;
CREATE TABLE IF NOT EXISTS report_month_config (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  first_day DATE NOT NULL,
  last_day DATE NOT NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO report_month_config (first_day, last_day)
VALUES ('2025-11-01', '2025-11-30');
