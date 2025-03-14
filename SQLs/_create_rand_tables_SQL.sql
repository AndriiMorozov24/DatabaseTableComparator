USE AdventureWorksLT2022;

DECLARE @DBSCHEMA NVARCHAR(128) = 'STIR';
DECLARE @FINALTABLENAME NVARCHAR(128) = 'FINAL';
DECLARE @i INT = 1;
DECLARE @max INT = 10;
DECLARE @date DATE = '2023-11-10';  -- Adjust as needed
DECLARE @sql NVARCHAR(MAX);
DECLARE @RandomNum INT;
DECLARE @iFormatted NVARCHAR(2);

-- Create schema if it doesn't exist
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = @DBSCHEMA)
BEGIN
    EXEC('CREATE SCHEMA [' + @DBSCHEMA + ']');
END;

WHILE @i <= @max
BEGIN
    -- Format the counter as two digits (e.g., 01, 02, etc.)
    SET @iFormatted = RIGHT('00' + CAST(@i AS NVARCHAR(2)), 2);
    SET @RandomNum = ABS(CHECKSUM(NEWID())) % 10 + 1;

    SET @sql = 
N'-- Drop existing tables
DROP TABLE IF EXISTS [' + @DBSCHEMA + '].[' + 'DET_SENT_' + @iFormatted + '_RAND' + '];
DROP TABLE IF EXISTS [' + @DBSCHEMA + '].[' + 'ACC_SENT_' + @iFormatted + '_RAND' + '];
DROP TABLE IF EXISTS [' + @DBSCHEMA + '].[' + 'REL_SENT_' + @iFormatted + '_RAND' + '];
DROP TABLE IF EXISTS [' + @DBSCHEMA + '].[' + 'ACC_REL_SENT_' + @iFormatted + '_RAND' + '];
DROP TABLE IF EXISTS [' + @DBSCHEMA + '].[' + 'ERR_LOG_' + @iFormatted + '_RAND' + '];
DROP TABLE IF EXISTS [' + @DBSCHEMA + '].[' + @FINALTABLENAME + '_' + @iFormatted + '_RAND' + '];

-- CUSTOMER DETAILS
CREATE TABLE [' + @DBSCHEMA + '].[' + 'DET_SENT_' + @iFormatted + '_RAND' + '] (
    WH_CUST_NO INT,
    CIF CHAR(10),
    NIP CHAR(10),
    REGON CHAR(10),
    NAZWA VARCHAR(50),
    PERIOD_DTE DATE DEFAULT ''' + CONVERT(NVARCHAR(10), @date, 23) + ''',
    PRIMARY KEY (WH_CUST_NO, CIF, PERIOD_DTE)
);

-- ACCOUNT DETAILS
CREATE TABLE [' + @DBSCHEMA + '].[' + 'ACC_SENT_' + @iFormatted + '_RAND' + '] (
    WH_CUST_NO INT,
    WH_ACC_NO INT,
    IBAN_NO VARCHAR(26),
    STA_CDE CHAR(1),
    PERIOD_DTE DATE DEFAULT ''' + CONVERT(NVARCHAR(10), @date, 23) + ''',
    PRIMARY KEY (WH_CUST_NO, WH_ACC_NO, PERIOD_DTE)
);

-- RELATIONSHIP DETAILS
CREATE TABLE [' + @DBSCHEMA + '].[' + 'REL_SENT_' + @iFormatted + '_RAND' + '] (
    WH_CUST_NO INT,
    WH_SECNDRY_CUST_NO INT,
    SECNDRY_CIF CHAR(10),
    REL_TYPE CHAR(1),
    REL_DESC VARCHAR(50),
    PERIOD_DTE DATE DEFAULT ''' + CONVERT(NVARCHAR(10), @date, 23) + ''',
    PRIMARY KEY (WH_CUST_NO, WH_SECNDRY_CUST_NO, PERIOD_DTE)
);

-- ACCOUNT RELATIONSHIP DETAILS
CREATE TABLE [' + @DBSCHEMA + '].[' + 'ACC_REL_SENT_' + @iFormatted + '_RAND' + '] (
    WH_CUST_NO INT,
    WH_ACC_NO INT,
    WH_SECNDRY_CUST_NO INT,
    PERIOD_DTE DATE DEFAULT ''' + CONVERT(NVARCHAR(10), @date, 23) + ''',
    PRIMARY KEY (WH_CUST_NO, WH_SECNDRY_CUST_NO, WH_ACC_NO, PERIOD_DTE)
);

-- Create ERR_LOG table
CREATE TABLE [' + @DBSCHEMA + '].[' + 'ERR_LOG_' + @iFormatted + '_RAND' + '] (
    ERR_ID INT IDENTITY(1,1) PRIMARY KEY,
    WH_CUST_NO INT,
    ERR_LVL VARCHAR(50),
    ERR_CDE VARCHAR(50),
    ERR_DESCR VARCHAR(50),
    FILE_GENERATION_DTE DATE
);

-- Insert sample data into DET_SENT
INSERT INTO [' + @DBSCHEMA + '].[' + 'DET_SENT_' + @iFormatted + '_RAND' + ']
    (WH_CUST_NO, CIF, NIP, REGON, NAZWA, PERIOD_DTE) VALUES
    (1, ''CIF1234567'', ''NIP1234567'', ''REG1234567'', ''Customer A'', ''2023-12-10''),
    (1, ''CIF1234567'', ''NIP1234567'', ''REG1234567'', ''Customer A'', ''2024-01-15''),
    (1, ''CIF1234567'', ''NIP1234567'', ''REG1234567'', ''Customer A'', ''2025-02-20'');

-- Insert sample data into ACC_SENT
INSERT INTO [' + @DBSCHEMA + '].[' + 'ACC_SENT_' + @iFormatted + '_RAND' + ']
    (WH_CUST_NO, WH_ACC_NO, IBAN_NO, STA_CDE, PERIOD_DTE)
SELECT 1, ABS(CHECKSUM(NEWID())) % 100000, 
       ''PL'' + RIGHT(''0000000000000000000'' + CAST(ABS(CHECKSUM(NEWID())) % 1000000000000000000 AS VARCHAR(24)), 24), 
       ''A'', PERIOD_DTE
FROM (VALUES (''2023-12-10''), (''2024-01-15'')) v(PERIOD_DTE)
CROSS JOIN (SELECT TOP ' + CAST(@RandomNum AS NVARCHAR(3)) + ' 1 AS n FROM sys.all_objects) AS T;

-- Insert sample data into REL_SENT
INSERT INTO [' + @DBSCHEMA + '].[' + 'REL_SENT_' + @iFormatted + '_RAND' + ']
    (WH_CUST_NO, WH_SECNDRY_CUST_NO, SECNDRY_CIF, REL_TYPE, REL_DESC, PERIOD_DTE)
SELECT 1, ABS(CHECKSUM(NEWID())) % 10000, 
       ''CIF'' + CAST(ABS(CHECKSUM(NEWID())) % 1000000 AS VARCHAR(6)), 
       ''B'', ''Relationship Desc'', PERIOD_DTE
FROM (VALUES (''2023-12-10''), (''2024-01-15'')) v(PERIOD_DTE)
CROSS JOIN (SELECT TOP ' + CAST(@RandomNum AS NVARCHAR(3)) + ' 1 AS n FROM sys.all_objects) AS T;

-- Insert into ACC_REL_SENT
INSERT INTO [' + @DBSCHEMA + '].[' + 'ACC_REL_SENT_' + @iFormatted + '_RAND' + ']
    (WH_CUST_NO, WH_ACC_NO, WH_SECNDRY_CUST_NO, PERIOD_DTE)
SELECT ACC.WH_CUST_NO, ACC.WH_ACC_NO, REL.WH_SECNDRY_CUST_NO, ACC.PERIOD_DTE
FROM [' + @DBSCHEMA + '].[' + 'ACC_SENT_' + @iFormatted + '_RAND' + '] AS ACC
JOIN [' + @DBSCHEMA + '].[' + 'REL_SENT_' + @iFormatted + '_RAND' + '] AS REL 
  ON ACC.WH_CUST_NO = REL.WH_CUST_NO AND ACC.PERIOD_DTE = REL.PERIOD_DTE;

-- Insert sample data into ERR_LOG
INSERT INTO [' + @DBSCHEMA + '].[' + 'ERR_LOG_' + @iFormatted + '_RAND' + ']
    (WH_CUST_NO, ERR_LVL, ERR_CDE, ERR_DESCR, FILE_GENERATION_DTE)
SELECT 1, ''Critical'', 
       CAST(ABS(CHECKSUM(NEWID())) % 10 + 1 AS VARCHAR(4)), 
       ''Error Description'', DATEADD(DAY, ABS(CHECKSUM(NEWID())) % 10, ''2023-12-10'')
FROM sys.all_objects WHERE object_id < 100;

DROP TABLE IF EXISTS #DET_ROW_NUM_PERIOD;
';

    BEGIN TRY
        EXEC sp_executesql @sql;
    END TRY
    BEGIN CATCH
        PRINT 'Error in SQL Execution: ' + ERROR_MESSAGE();
    END CATCH;

    SET @i = @i + 1;
END;
