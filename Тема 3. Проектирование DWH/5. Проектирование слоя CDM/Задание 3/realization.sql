ALTER TABLE cdm.dm_settlement_report 
ADD CONSTRAINT dm_settlement_report_settlement_date_check
CHECK (settlement_date >= '01.01.2022' AND settlement_date < '01.01.2500');