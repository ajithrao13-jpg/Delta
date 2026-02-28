
CREATE TABLE dbo.TGT_SPW_ACCOUNT_DETAIL
(
    ID                  BIGINT          NOT NULL,
    PACKAGE_ID          BIGINT          NOT NULL,
    account_number      VARCHAR(20)     NULL,
    open_date           DATE            NULL,
    market_value        DECIMAL(18,2)   NULL,
    is_pledged          BIT             NOT NULL,
    SYSTEM_DATETIME     DATETIME2(7)    NOT NULL,
    row_hash            VARBINARY(32)   NOT NULL,  -- SHA2_256 = 32 bytes

    CONSTRAINT PK_TGT_SPW_ACCOUNT_DETAIL 
        PRIMARY KEY (ID)
);

CREATE TABLE dbo.TGT_SPW_POSITION_DETAIL
(
    ID                  BIGINT          NOT NULL,
    PACKAGE_ID          BIGINT          NOT NULL,
    account_number      VARCHAR(20)     NULL,
    cusip               VARCHAR(9)      NULL,
    symbol              VARCHAR(9)      NULL,
    market_price        DECIMAL(18,4)   NULL,
    quantity            DECIMAL(18,4)   NULL,
    market_value        DECIMAL(18,2)   NULL,
    valuation_date      DATE            NULL,
    SYSTEM_DATETIME     DATETIME2(7)    NOT NULL,
    row_hash            VARBINARY(32)   NOT NULL,

    CONSTRAINT PK_TGT_SPW_POSITION_DETAIL 
        PRIMARY KEY (ID)
);