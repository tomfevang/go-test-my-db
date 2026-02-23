CREATE TABLE BenchCompany (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(100) NOT NULL
);

CREATE TABLE DimSeverity (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(20) NOT NULL
);

INSERT INTO DimSeverity (name) VALUES ('LOW'), ('MEDIUM'), ('HIGH'), ('CRITICAL');

CREATE TABLE DimStatus (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(20) NOT NULL
);

INSERT INTO DimStatus (name) VALUES ('OPEN'), ('INVESTIGATING'), ('RESOLVED'), ('DISMISSED');

CREATE TABLE DimRegion (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(10) NOT NULL
);

INSERT INTO DimRegion (name) VALUES ('EMEA'), ('APAC'), ('NAM'), ('LATAM');

CREATE TABLE DimSource (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(10) NOT NULL
);

INSERT INTO DimSource (name) VALUES ('ERP'), ('BANK'), ('MANUAL');

CREATE TABLE AccountCheckStar (
  id INT AUTO_INCREMENT PRIMARY KEY,
  companyId INT NOT NULL,
  anomalyType VARCHAR(50) NOT NULL,
  accountNumber VARCHAR(20) NOT NULL,
  regionId INT NOT NULL,
  sourceId INT NOT NULL,
  INDEX idx_company (companyId),
  INDEX idx_region (regionId),
  INDEX idx_source (sourceId),
  INDEX idx_company_region (companyId, regionId),
  INDEX idx_region_source (regionId, sourceId)
);

CREATE TABLE AccountAnomalyStar (
  id INT AUTO_INCREMENT PRIMARY KEY,
  accountCheckId INT NOT NULL,
  companyId INT NOT NULL,
  severityId INT NOT NULL,
  statusId INT NOT NULL,
  date DATE NOT NULL,
  amount INT NOT NULL,
  message VARCHAR(255) NOT NULL,
  INDEX idx_account_check (accountCheckId),
  INDEX idx_company (companyId),
  INDEX idx_severity (severityId),
  INDEX idx_severity_date (severityId, date),
  INDEX idx_severity_status_date (severityId, statusId, date),
  INDEX idx_company_severity (companyId, severityId),
  INDEX idx_company_severity_date (companyId, severityId, date),
  INDEX idx_company_severity_status_date (companyId, severityId, statusId, date)
);
