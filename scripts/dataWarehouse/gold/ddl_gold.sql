/*
===============================================================================
DDL + Load Procedures: Gold Layer — Physical Tables (Star Schema)
===============================================================================
  Includes:
    1. Physical dimension & fact tables with all indexes
    2. Clustered columnstore index on fact_sales
    3. dw_load_log control table
    4. Load procedures:
         load_dim_date
         load_dim_customers_scd   (SCD Type 2)
         load_dim_products_scd    (SCD Type 2)
         load_fact_sales
===============================================================================
*/

-- ============================================================
-- SECTION 1: Physical Tables
-- ============================================================

-- ------------------------------------------------------------
-- 1a. dim_customers  (SCD Type 2)
-- ------------------------------------------------------------
IF OBJECT_ID('gold.dim_customers', 'U') IS NOT NULL DROP TABLE gold.dim_customers;
GO

CREATE TABLE gold.dim_customers (
    customer_key     INT            NOT NULL IDENTITY(1,1),
    customer_id      INT            NOT NULL,           -- BK: crm_cust_info.cst_id
    customer_number  NVARCHAR(50)   NOT NULL,           -- BK: crm_cust_info.cst_key
    first_name       NVARCHAR(50)   NULL,
    last_name        NVARCHAR(50)   NULL,
    country          NVARCHAR(50)   NULL,
    marital_status   NVARCHAR(20)   NULL,
    gender           NVARCHAR(10)   NULL,
    birthdate        DATE           NULL,
    create_date      DATE           NULL,
    -- SCD Type 2 metadata
    scd_start_date   DATE           NOT NULL DEFAULT CAST(GETDATE() AS DATE),
    scd_end_date     DATE           NULL,               -- NULL = current record
    scd_is_current   BIT            NOT NULL DEFAULT 1,
    -- Audit
    dw_insert_date   DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    dw_update_date   DATETIME2      NULL,
    CONSTRAINT pk_dim_customers PRIMARY KEY CLUSTERED (customer_key)
);
GO

-- Lookup index on BK + currency flag (used heavily in fact loads)
CREATE NONCLUSTERED INDEX ix_dim_customers_bk
    ON gold.dim_customers (customer_id, scd_is_current)
    INCLUDE (customer_key);
GO

-- ------------------------------------------------------------
-- 1b. dim_products  (SCD Type 2)
-- ------------------------------------------------------------
IF OBJECT_ID('gold.dim_products', 'U') IS NOT NULL DROP TABLE gold.dim_products;
GO

CREATE TABLE gold.dim_products (
    product_key      INT            NOT NULL IDENTITY(1,1),
    product_id       INT            NOT NULL,           -- BK: crm_prd_info.prd_id
    product_number   NVARCHAR(50)   NOT NULL,           -- BK: crm_prd_info.prd_key
    product_name     NVARCHAR(100)  NULL,
    category_id      NVARCHAR(50)   NULL,
    category         NVARCHAR(50)   NULL,
    subcategory      NVARCHAR(50)   NULL,
    maintenance      NVARCHAR(50)   NULL,
    cost             DECIMAL(18,2)  NULL,
    product_line     NVARCHAR(50)   NULL,
    -- SCD Type 2 metadata
    scd_start_date   DATE           NOT NULL DEFAULT CAST(GETDATE() AS DATE),
    scd_end_date     DATE           NULL,
    scd_is_current   BIT            NOT NULL DEFAULT 1,
    -- Audit
    dw_insert_date   DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    dw_update_date   DATETIME2      NULL,
    CONSTRAINT pk_dim_products PRIMARY KEY CLUSTERED (product_key)
);
GO

CREATE NONCLUSTERED INDEX ix_dim_products_bk
    ON gold.dim_products (product_id, scd_is_current)
    INCLUDE (product_key);
GO

CREATE NONCLUSTERED INDEX ix_dim_products_number
    ON gold.dim_products (product_number, scd_is_current)
    INCLUDE (product_key);
GO

-- ------------------------------------------------------------
-- 1c. dim_date  (static calendar — no SCD needed)
-- ------------------------------------------------------------
IF OBJECT_ID('gold.dim_date', 'U') IS NOT NULL DROP TABLE gold.dim_date;
GO

CREATE TABLE gold.dim_date (
    date_key         DATE           NOT NULL,           -- PK is the date itself (readable)
    year             SMALLINT       NOT NULL,
    quarter          TINYINT        NOT NULL,
    month_number     TINYINT        NOT NULL,
    month_name       NVARCHAR(10)   NOT NULL,
    week_number      TINYINT        NOT NULL,
    day_of_week      TINYINT        NOT NULL,           -- 1=Sun … 7=Sat (DATEFIRST-aware)
    day_name         NVARCHAR(10)   NOT NULL,
    is_weekend       BIT            NOT NULL DEFAULT 0,
    is_holiday       BIT            NOT NULL DEFAULT 0,
    CONSTRAINT pk_dim_date PRIMARY KEY CLUSTERED (date_key)
);
GO

-- ------------------------------------------------------------
-- 1d. fact_sales
-- ------------------------------------------------------------
IF OBJECT_ID('gold.fact_sales', 'U') IS NOT NULL DROP TABLE gold.fact_sales;
GO

CREATE TABLE gold.fact_sales (
    order_number     NVARCHAR(50)   NOT NULL,
    product_key      INT            NOT NULL,
    customer_key     INT            NOT NULL,
    date_key         DATE           NOT NULL,
    order_date       DATE           NULL,
    shipping_date    DATE           NULL,
    due_date         DATE           NULL,
    sales_amount     DECIMAL(18,2)  NULL,
    quantity         INT            NULL,
    price            DECIMAL(18,2)  NULL,
    -- Audit
    dw_insert_date   DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_fact_sales PRIMARY KEY NONCLUSTERED (order_number),
    CONSTRAINT fk_fact_sales_product  FOREIGN KEY (product_key)  REFERENCES gold.dim_products  (product_key),
    CONSTRAINT fk_fact_sales_customer FOREIGN KEY (customer_key) REFERENCES gold.dim_customers (customer_key),
    CONSTRAINT fk_fact_sales_date     FOREIGN KEY (date_key)     REFERENCES gold.dim_date      (date_key)
);
GO

-- Clustered columnstore for analytics workloads
CREATE CLUSTERED COLUMNSTORE INDEX cci_fact_sales
    ON gold.fact_sales;
GO

-- Supporting row-store indexes for FK lookups / selective queries
CREATE NONCLUSTERED INDEX ix_fact_sales_customer
    ON gold.fact_sales (customer_key);
GO

CREATE NONCLUSTERED INDEX ix_fact_sales_product
    ON gold.fact_sales (product_key);
GO

CREATE NONCLUSTERED INDEX ix_fact_sales_date
    ON gold.fact_sales (date_key);
GO

-- ============================================================
-- SECTION 2: Control / Audit Table
-- ============================================================
IF OBJECT_ID('gold.dw_load_log', 'U') IS NOT NULL DROP TABLE gold.dw_load_log;
GO

CREATE TABLE gold.dw_load_log (
    log_id           INT            NOT NULL IDENTITY(1,1),
    procedure_name   NVARCHAR(128)  NOT NULL,
    load_start       DATETIME2      NOT NULL,
    load_end         DATETIME2      NULL,
    rows_inserted    INT            NULL DEFAULT 0,
    rows_updated     INT            NULL DEFAULT 0,
    rows_expired     INT            NULL DEFAULT 0,     -- SCD Type 2 expirations
    status           NVARCHAR(20)   NOT NULL DEFAULT 'RUNNING',   -- RUNNING | SUCCESS | FAILED
    error_message    NVARCHAR(4000) NULL,
    CONSTRAINT pk_dw_load_log PRIMARY KEY CLUSTERED (log_id)
);
GO