CREATE TABLE AccountAnomalyInv (
  id INT AUTO_INCREMENT PRIMARY KEY,
  accountCheckId INT NOT NULL,
  date DATE NOT NULL,
  message VARCHAR(255) NOT NULL,
  INDEX idx_account_check (accountCheckId)
);

CREATE TABLE AnomalySeverity (
  id INT AUTO_INCREMENT PRIMARY KEY,
  anomalyId INT NOT NULL,
  severity VARCHAR(20) NOT NULL,
  INDEX idx_anomaly (anomalyId),
  INDEX idx_severity (severity),
  INDEX idx_severity_anomaly (severity, anomalyId)
);

CREATE TABLE AnomalyStatus (
  id INT AUTO_INCREMENT PRIMARY KEY,
  anomalyId INT NOT NULL,
  status VARCHAR(20) NOT NULL,
  INDEX idx_anomaly (anomalyId),
  INDEX idx_status (status),
  INDEX idx_status_anomaly (status, anomalyId)
);

CREATE TABLE AnomalyAmount (
  id INT AUTO_INCREMENT PRIMARY KEY,
  anomalyId INT NOT NULL,
  amount INT NOT NULL,
  INDEX idx_anomaly (anomalyId),
  INDEX idx_amount (amount),
  INDEX idx_amount_anomaly (amount, anomalyId)
);
