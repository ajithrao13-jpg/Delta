CREATE OR ALTER PROCEDURE dbo.SP_DELTA_SPW_ACCOUNT_DETAIL
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH LatestStage AS (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY PACKAGE_ID
                       ORDER BY SYSTEM_DATETIME DESC
                   ) AS rn
            FROM dbo.STG_SPW_ACCOUNT_DETAIL
        ) s
        WHERE rn = 1
    )

    MERGE dbo.TGT_SPW_ACCOUNT_DETAIL AS T
    USING LatestStage AS S
        ON T.PACKAGE_ID = S.PACKAGE_ID   -- 🔑 match by PACKAGE_ID

    -- 🔄 UPDATE if hash changed
    WHEN MATCHED AND T.row_hash <> S.row_hash
    THEN UPDATE SET
        T.ID              = S.ID,
        T.account_number  = S.account_number,
        T.open_date       = S.open_date,
        T.market_value    = S.market_value,
        T.is_pledged      = S.is_pledged,
        T.SYSTEM_DATETIME = S.SYSTEM_DATETIME,
        T.row_hash        = S.row_hash

    -- ➕ INSERT new PACKAGE_ID
    WHEN NOT MATCHED BY TARGET
    THEN INSERT (
        ID, PACKAGE_ID, account_number, open_date,
        market_value, is_pledged, SYSTEM_DATETIME, row_hash
    )
    VALUES (
        S.ID, S.PACKAGE_ID, S.account_number, S.open_date,
        S.market_value, S.is_pledged, S.SYSTEM_DATETIME, S.row_hash
    );

END;
GO	

CREATE OR ALTER PROCEDURE dbo.SP_DELTA_SPW_POSITION_DETAIL
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH LatestStage AS (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY PACKAGE_ID
                       ORDER BY SYSTEM_DATETIME DESC
                   ) AS rn
            FROM dbo.STG_SPW_POSITION_DETAIL
        ) s
        WHERE rn = 1
    )

    MERGE dbo.TGT_SPW_POSITION_DETAIL AS T
    USING LatestStage AS S
        ON T.PACKAGE_ID = S.PACKAGE_ID   -- 🔑 match by PACKAGE_ID

    -- 🔄 UPDATE if hash changed
    WHEN MATCHED AND T.row_hash <> S.row_hash
    THEN UPDATE SET
        T.ID              = S.ID,
        T.account_number  = S.account_number,
        T.cusip           = S.cusip,
        T.symbol          = S.symbol,
        T.market_price    = S.market_price,
        T.quantity        = S.quantity,
        T.market_value    = S.market_value,
        T.valuation_date  = S.valuation_date,
        T.SYSTEM_DATETIME = S.SYSTEM_DATETIME,
        T.row_hash        = S.row_hash

    -- ➕ INSERT new PACKAGE_ID
    WHEN NOT MATCHED BY TARGET
    THEN INSERT (
        ID, PACKAGE_ID, account_number, cusip, symbol,
        market_price, quantity, market_value, valuation_date,
        SYSTEM_DATETIME, row_hash
    )
    VALUES (
        S.ID, S.PACKAGE_ID, S.account_number, S.cusip, S.symbol,
        S.market_price, S.quantity, S.market_value, S.valuation_date,
        S.SYSTEM_DATETIME, S.row_hash
    );

END;
GO