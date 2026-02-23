CREATE TABLE BenchCompany (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(100) NOT NULL
);

CREATE TABLE AccountCheck (
  id INT AUTO_INCREMENT PRIMARY KEY,
  companyId INT NOT NULL,
  details JSON NOT NULL,
  anomalyType VARCHAR(50) NOT NULL,
  accountNumber VARCHAR(20) NOT NULL,
  _region VARCHAR(10) GENERATED ALWAYS AS (details->>'$.region') STORED INVISIBLE,
  _source VARCHAR(10) GENERATED ALWAYS AS (details->>'$.source') STORED INVISIBLE,
  INDEX idx_company (companyId),
  INDEX idx_region (_region),
  INDEX idx_source (_source),
  INDEX idx_company_region (companyId, _region)
);

CREATE TABLE AccountAnomaly (
  id INT AUTO_INCREMENT PRIMARY KEY,
  accountCheckId INT NOT NULL,
  companyId INT NOT NULL,
  details JSON NOT NULL,
  date DATE NOT NULL,
  status VARCHAR(20) NOT NULL,
  _severity VARCHAR(10) GENERATED ALWAYS AS (details->>'$.severity') STORED INVISIBLE,
  _amount INT GENERATED ALWAYS AS (CAST(details->>'$.amount' AS SIGNED)) STORED INVISIBLE,
  INDEX idx_account_check (accountCheckId),
  INDEX idx_company (companyId),
  INDEX idx_severity (_severity),
  INDEX idx_severity_date (_severity, date),
  INDEX idx_severity_status_date (_severity, status, date),
  INDEX idx_company_severity (companyId, _severity),
  INDEX idx_company_severity_date (companyId, _severity, date),
  INDEX idx_company_severity_status_date (companyId, _severity, status, date)
);
