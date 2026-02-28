CREATE TABLE RAW_SPW_ACCOUNT_DETAIL (
  ID bigint IDENTITY(1, 1) NOT NULL, 
  PACKAGE_ID bigint NOT NULL, 
  account_number varchar(20) NULL, 
  address_line_1 varchar(40) NULL, 
  address_line_2 varchar(40) NULL, 
  address_line_3 varchar(40) NULL, 
  address_line_4 varchar(40) NULL, 
  address_line_5 varchar(40) NULL, 
  address_line_6 varchar(40) NULL, 
  open_date varchar(10) NULL, 
  market_value varchar(15) NULL, 
  old_account_number varchar(20) NULL, 
  pledge_indicator varchar(1) NULL, 
  SYSTEM_DATETIME datetime2(7) NOT NULL, 
  CONSTRAINT PK_RAW_SPW_ACCOUNT_DETAIL PRIMARY KEY CLUSTERED (ID ASC) WITH (
    STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF
  ) ON [PRIMARY]
) ON [PRIMARY];
GO 
ALTER TABLE 
  RAW_SPW_ACCOUNT_DETAIL 
ADD 
  DEFAULT (
    sysutcdatetime()
  ) FOR SYSTEM_DATETIME;
GO 
--ALTER TABLE 
--  RAW_SPW_ACCOUNT_DETAIL WITH CHECK 
--ADD 
--  CONSTRAINT fk_rawspwaccountdetail_syspackagecontrol FOREIGN KEY (PACKAGE_ID) REFERENCES SYS_PACKAGE_CONTROL (PACKAGE_ID);
--GO 
--ALTER TABLE 
--  RAW_SPW_ACCOUNT_DETAIL CHECK CONSTRAINT fk_rawspwaccountdetail_syspackagecontrol;
--GO


CREATE TABLE [dbo].[RAW_SPW_POSITION_DETAIL] (
    [ID] [bigint] IDENTITY(1,1) NOT NULL,
    [PACKAGE_ID] [bigint] NOT NULL,
    [account_number] [varchar](20) NULL,
    [cusip] [varchar](9) NULL,
    [symbol] [varchar](9) NULL,
    [market_price] [varchar](15) NULL,
    [quantity] [varchar](15) NULL,
    [market_value] [varchar](15) NULL,
    [positio_desc] [varchar](30) NULL,
    [product_type_code] [varchar](5) NULL,
    [date_last_valuation] [varchar](8) NULL,
    [SYSTEM_DATETIME] [datetime2](7) NOT NULL,
    CONSTRAINT [PK_RAW_SPW_POSITION_DETAIL] PRIMARY KEY CLUSTERED ([ID] ASC)
    WITH (
        STATISTICS_NORECOMPUTE = OFF, 
        IGNORE_DUP_KEY = OFF, 
        OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF
    ) ON [PRIMARY]
) ON [PRIMARY];
GO

-- Add Default Constraint for SYSTEM_DATETIME
ALTER TABLE [dbo].[RAW_SPW_POSITION_DETAIL] 
ADD DEFAULT (sysutcdatetime()) FOR [SYSTEM_DATETIME];
GO

-- Add Foreign Key Constraint
--ALTER TABLE [dbo].[RAW_SPW_POSITION_DETAIL] WITH CHECK 
--ADD CONSTRAINT [fk_rawspwpositiondetail_syspackagecontrol] 
--FOREIGN KEY([PACKAGE_ID]) REFERENCES [dbo].[SYS_PACKAGE_CONTROL] ([PACKAGE_ID]);
--GO

-- Enable the Constraint
--ALTER TABLE [dbo].[RAW_SPW_POSITION_DETAIL] 
--CHECK CONSTRAINT [fk_rawspwpositiondetail_syspackagecontrol];
--GO


INSERT INTO RAW_SPW_ACCOUNT_DETAIL
(PACKAGE_ID, account_number, address_line_1, open_date, market_value, old_account_number, pledge_indicator)
VALUES
(1001, 'ACC1001', '12 Main St', '20240101', '150000.50', NULL, 'N'),
(1002, 'ACC1002', '45 Park Ave', '20240215', '250000.00', NULL, 'Y'),
(1003, 'ACC1003', '78 Lake View', '20240310', '98000.75', NULL, 'N'),
(1004, 'ACC1004', '9 Hill Road', '20240405', '500500.25', NULL, 'Y');

 INSERT INTO RAW_SPW_POSITION_DETAIL
(PACKAGE_ID, account_number, cusip, symbol, market_price, quantity, market_value, positio_desc, product_type_code, date_last_valuation)
VALUES
(2001, 'ACC1001', '123456789', 'AAPL', '185.50', '10', '1855.00', 'Apple Stock', 'EQ', '20240115'),
(2002, 'ACC1002', '987654321', 'MSFT', '320.75', '5', '1603.75', 'Microsoft Stock', 'EQ', '20240220'),
(2003, 'ACC1003', '456789123', 'GOOGL', '140.20', '8', '1121.60', 'Google Stock', 'EQ', '20240318'),
(2004, 'ACC1004', '789123456', 'TSLA', '250.00', '6', '1500.00', 'Tesla Stock', 'EQ', '20240422');



/* ============================================================
   RAW_SPW_ACCOUNT_DETAIL
   ============================================================ */

-- ➕ NEW PACKAGE_ID → should INSERT downstream
INSERT INTO dbo.RAW_SPW_ACCOUNT_DETAIL
(PACKAGE_ID, account_number, address_line_1, open_date, market_value, pledge_indicator)
VALUES
(1005, 'ACC1005', '22 River Road', '20240501', '120000.00', 'N');


-- 🔁 EXISTING PACKAGE_ID → should UPDATE downstream
-- Changes:
-- market_value: 150000.50 → 175000.00
-- pledge_indicator: N → Y
INSERT INTO dbo.RAW_SPW_ACCOUNT_DETAIL
(PACKAGE_ID, account_number, address_line_1, open_date, market_value, pledge_indicator)
VALUES
(1001, 'ACC1001', '12 Main St', '20240101', '175000.00', 'Y');



/* ============================================================
   RAW_SPW_POSITION_DETAIL
   ============================================================ */

-- ➕ NEW PACKAGE_ID → should INSERT downstream
INSERT INTO dbo.RAW_SPW_POSITION_DETAIL
(PACKAGE_ID, account_number, cusip, symbol, market_price, quantity, market_value, positio_desc, product_type_code, date_last_valuation)
VALUES
(2005, 'ACC1005', '555666777', 'AMZN', '178.25', '4', '713.00', 'Amazon Stock', 'EQ', '20240501');


-- 🔁 EXISTING PACKAGE_ID → should UPDATE downstream
-- Changes:
-- market_price: 185.50 → 190.00
-- market_value: 1855.00 → 1900.00
-- date_last_valuation updated
INSERT INTO dbo.RAW_SPW_POSITION_DETAIL
(PACKAGE_ID, account_number, cusip, symbol, market_price, quantity, market_value, positio_desc, product_type_code, date_last_valuation)
VALUES
(2001, 'ACC1001', '123456789', 'AAPL', '190.00', '10', '1900.00', 'Apple Stock', 'EQ', '20240502');