

-- ============================================================
-- SECTION 3: Load Procedures
-- ============================================================

-- ------------------------------------------------------------
-- 3a. load_dim_date
--     Populates dim_date for a date range.
--     Safe to re-run (MERGE on date_key — no duplicates).
-- ------------------------------------------------------------
IF OBJECT_ID('gold.load_dim_date', 'P') IS NOT NULL DROP PROCEDURE gold.load_dim_date;
GO

CREATE PROCEDURE gold.load_dim_date
    @start_date DATE = '2000-01-01',
    @end_date   DATE = '2030-12-31'
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @log_id       INT;
    DECLARE @load_start   DATETIME2 = SYSUTCDATETIME();
    DECLARE @rows_ins     INT = 0;

    -- Open log entry
    INSERT INTO gold.dw_load_log (procedure_name, load_start, status)
    VALUES ('gold.load_dim_date', @load_start, 'RUNNING');
    SET @log_id = SCOPE_IDENTITY();

    BEGIN TRY
        -- Build a numbers CTE to generate every date in range
        ;WITH numbers AS (
            SELECT TOP (DATEDIFF(DAY, @start_date, @end_date) + 1)
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
            FROM sys.all_columns a CROSS JOIN sys.all_columns b
        ),
        calendar AS (
            SELECT
                DATEADD(DAY, n, @start_date) AS dt
            FROM numbers
        )
        MERGE gold.dim_date AS tgt
        USING (
            SELECT
                dt                                                  AS date_key,
                YEAR(dt)                                            AS year,
                DATEPART(QUARTER, dt)                               AS quarter,
                MONTH(dt)                                           AS month_number,
                DATENAME(MONTH, dt)                                 AS month_name,
                DATEPART(WEEK, dt)                                  AS week_number,
                DATEPART(WEEKDAY, dt)                               AS day_of_week,
                DATENAME(WEEKDAY, dt)                               AS day_name,
                CASE WHEN DATEPART(WEEKDAY, dt) IN (1,7) THEN 1 ELSE 0 END AS is_weekend,
                CAST(0 AS BIT)                                      AS is_holiday  -- extend as needed
            FROM calendar
        ) AS src ON tgt.date_key = src.date_key
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (date_key, year, quarter, month_number, month_name,
                    week_number, day_of_week, day_name, is_weekend, is_holiday)
            VALUES (src.date_key, src.year, src.quarter, src.month_number, src.month_name,
                    src.week_number, src.day_of_week, src.day_name, src.is_weekend, src.is_holiday);

        SET @rows_ins = @@ROWCOUNT;

        UPDATE gold.dw_load_log
        SET load_end = SYSUTCDATETIME(), rows_inserted = @rows_ins, status = 'SUCCESS'
        WHERE log_id = @log_id;

        PRINT 'load_dim_date: ' + CAST(@rows_ins AS NVARCHAR) + ' rows inserted.';
    END TRY
    BEGIN CATCH
        UPDATE gold.dw_load_log
        SET load_end = SYSUTCDATETIME(), status = 'FAILED',
            error_message = ERROR_MESSAGE()
        WHERE log_id = @log_id;

        THROW;
    END CATCH
END;
GO


-- ------------------------------------------------------------
-- 3b. load_dim_customers_scd   (SCD Type 2)
--     Source: silver.crm_cust_info, silver.erp_cust_az12,
--             silver.erp_loc_a101
--     Change-tracking columns: country, marital_status, gender
-- ------------------------------------------------------------
IF OBJECT_ID('gold.load_dim_customers_scd', 'P') IS NOT NULL
    DROP PROCEDURE gold.load_dim_customers_scd;
GO

CREATE PROCEDURE gold.load_dim_customers_scd
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @log_id      INT;
    DECLARE @load_start  DATETIME2 = SYSUTCDATETIME();
    DECLARE @today       DATE      = CAST(GETDATE() AS DATE);
    DECLARE @rows_ins    INT = 0;
    DECLARE @rows_upd    INT = 0;   -- rows expired (new version inserted)

    INSERT INTO gold.dw_load_log (procedure_name, load_start, status)
    VALUES ('gold.load_dim_customers_scd', @load_start, 'RUNNING');
    SET @log_id = SCOPE_IDENTITY();

    BEGIN TRY
        -- ── Stage: resolve source into clean customer records ──────────────
        IF OBJECT_ID('tempdb..#stg_customers') IS NOT NULL DROP TABLE #stg_customers;

        SELECT
            ci.cst_id                           AS customer_id,
            ci.cst_key                          AS customer_number,
            NULLIF(LTRIM(RTRIM(ci.cst_firstname)), '')  AS first_name,
            NULLIF(LTRIM(RTRIM(ci.cst_lastname)),  '')  AS last_name,
            NULLIF(la.cntry, 'n/a')             AS country,
            NULLIF(ci.cst_marital_status, 'n/a') AS marital_status,
            CASE
                WHEN NULLIF(ci.cst_gndr, 'n/a') IS NOT NULL THEN ci.cst_gndr
                ELSE COALESCE(NULLIF(ca.gen, 'n/a'), 'n/a')
            END                                 AS gender,
            ca.bdate                            AS birthdate,
            ci.cst_create_date                  AS create_date
        INTO #stg_customers
        FROM silver.crm_cust_info ci
        LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
        LEFT JOIN silver.erp_loc_a101   la ON ci.cst_key = la.cid;

        -- ── Expire changed records ────────────────────────────────────────
        UPDATE tgt
        SET    scd_end_date   = DATEADD(DAY, -1, @today),
               scd_is_current = 0,
               dw_update_date = SYSUTCDATETIME()
        FROM   gold.dim_customers tgt
        INNER JOIN #stg_customers src
            ON  tgt.customer_id   = src.customer_id
            AND tgt.scd_is_current = 1
        WHERE  -- NULL-safe comparison for tracked attributes
               ISNULL(tgt.country,        '<<NULL>>') <> ISNULL(src.country,        '<<NULL>>')
            OR ISNULL(tgt.marital_status, '<<NULL>>') <> ISNULL(src.marital_status, '<<NULL>>')
            OR ISNULL(tgt.gender,         '<<NULL>>') <> ISNULL(src.gender,         '<<NULL>>');

        SET @rows_upd = @@ROWCOUNT;

        -- ── Insert new / changed records ──────────────────────────────────
        INSERT INTO gold.dim_customers
               (customer_id, customer_number, first_name, last_name,
                country, marital_status, gender, birthdate, create_date,
                scd_start_date, scd_end_date, scd_is_current, dw_insert_date)
        SELECT  src.customer_id, src.customer_number, src.first_name, src.last_name,
                src.country, src.marital_status, src.gender, src.birthdate, src.create_date,
                @today, NULL, 1, SYSUTCDATETIME()
        FROM   #stg_customers src
        WHERE  -- New BK entirely
               NOT EXISTS (
                   SELECT 1 FROM gold.dim_customers
                   WHERE  customer_id = src.customer_id
               )
            OR -- Existing BK but current record was just expired
               EXISTS (
                   SELECT 1 FROM gold.dim_customers
                   WHERE  customer_id    = src.customer_id
                     AND  scd_is_current = 0
                     AND  scd_end_date   = DATEADD(DAY, -1, @today)
                     AND  dw_update_date >= @load_start          -- expired this run
               );

        SET @rows_ins = @@ROWCOUNT;

        UPDATE gold.dw_load_log
        SET load_end = SYSUTCDATETIME(), rows_inserted = @rows_ins,
            rows_expired = @rows_upd, status = 'SUCCESS'
        WHERE log_id = @log_id;

        PRINT 'load_dim_customers_scd — inserted: ' + CAST(@rows_ins AS NVARCHAR)
            + '  expired: '  + CAST(@rows_upd AS NVARCHAR);
    END TRY
    BEGIN CATCH
        UPDATE gold.dw_load_log
        SET load_end = SYSUTCDATETIME(), status = 'FAILED',
            error_message = ERROR_MESSAGE()
        WHERE log_id = @log_id;

        THROW;
    END CATCH
END;
GO


-- ------------------------------------------------------------
-- 3c. load_dim_products_scd   (SCD Type 2)
--     Source: silver.crm_prd_info, silver.erp_px_cat_g1v2
--     Only active products (prd_end_dt IS NULL) are treated
--     as current candidates; historical rows expire naturally.
--     Change-tracking columns: product_name, cost, product_line,
--                              category, subcategory
-- ------------------------------------------------------------
IF OBJECT_ID('gold.load_dim_products_scd', 'P') IS NOT NULL
    DROP PROCEDURE gold.load_dim_products_scd;
GO

CREATE PROCEDURE gold.load_dim_products_scd
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @log_id      INT;
    DECLARE @load_start  DATETIME2 = SYSUTCDATETIME();
    DECLARE @today       DATE      = CAST(GETDATE() AS DATE);
    DECLARE @rows_ins    INT = 0;
    DECLARE @rows_upd    INT = 0;

    INSERT INTO gold.dw_load_log (procedure_name, load_start, status)
    VALUES ('gold.load_dim_products_scd', @load_start, 'RUNNING');
    SET @log_id = SCOPE_IDENTITY();

    BEGIN TRY
        IF OBJECT_ID('tempdb..#stg_products') IS NOT NULL DROP TABLE #stg_products;

        SELECT
            pn.prd_id                           AS product_id,
            pn.prd_key                          AS product_number,
            NULLIF(LTRIM(RTRIM(pn.prd_nm)), '') AS product_name,
            pn.cat_id                           AS category_id,
            NULLIF(pc.cat,    'n/a')            AS category,
            NULLIF(pc.subcat, 'n/a')            AS subcategory,
            pc.maintenance                      AS maintenance,
            pn.prd_cost                         AS cost,
            NULLIF(pn.prd_line, 'n/a')          AS product_line
        INTO #stg_products
        FROM silver.crm_prd_info pn
        LEFT JOIN silver.erp_px_cat_g1v2 pc ON pn.cat_id = pc.id
        WHERE pn.prd_end_dt IS NULL;    -- active products only

        -- ── Expire changed records ────────────────────────────────────────
        UPDATE tgt
        SET    scd_end_date   = DATEADD(DAY, -1, @today),
               scd_is_current = 0,
               dw_update_date = SYSUTCDATETIME()
        FROM   gold.dim_products tgt
        INNER JOIN #stg_products src
            ON  tgt.product_id    = src.product_id
            AND tgt.scd_is_current = 1
        WHERE  ISNULL(tgt.product_name, '<<NULL>>') <> ISNULL(src.product_name, '<<NULL>>')
            OR ISNULL(CAST(tgt.cost AS NVARCHAR(30)), '<<NULL>>') <> ISNULL(CAST(src.cost AS NVARCHAR(30)), '<<NULL>>')
            OR ISNULL(tgt.product_line,  '<<NULL>>') <> ISNULL(src.product_line,  '<<NULL>>')
            OR ISNULL(tgt.category,      '<<NULL>>') <> ISNULL(src.category,      '<<NULL>>')
            OR ISNULL(tgt.subcategory,   '<<NULL>>') <> ISNULL(src.subcategory,   '<<NULL>>');

        SET @rows_upd = @@ROWCOUNT;

        -- ── Also expire dim rows whose source product was end-dated ───────
        UPDATE tgt
        SET    scd_end_date   = DATEADD(DAY, -1, @today),
               scd_is_current = 0,
               dw_update_date = SYSUTCDATETIME()
        FROM   gold.dim_products tgt
        WHERE  tgt.scd_is_current = 1
          AND  NOT EXISTS (
                   SELECT 1 FROM #stg_products s
                   WHERE  s.product_id = tgt.product_id
               );

        -- ── Insert new / changed records ──────────────────────────────────
        INSERT INTO gold.dim_products
               (product_id, product_number, product_name, category_id,
                category, subcategory, maintenance, cost, product_line,
                scd_start_date, scd_end_date, scd_is_current, dw_insert_date)
        SELECT  src.product_id, src.product_number, src.product_name, src.category_id,
                src.category, src.subcategory, src.maintenance, src.cost, src.product_line,
                @today, NULL, 1, SYSUTCDATETIME()
        FROM   #stg_products src
        WHERE  NOT EXISTS (
                   SELECT 1 FROM gold.dim_products
                   WHERE  product_id = src.product_id
               )
            OR EXISTS (
                   SELECT 1 FROM gold.dim_products
                   WHERE  product_id    = src.product_id
                     AND  scd_is_current = 0
                     AND  scd_end_date   = DATEADD(DAY, -1, @today)
                     AND  dw_update_date >= @load_start
               );

        SET @rows_ins = @@ROWCOUNT;

        UPDATE gold.dw_load_log
        SET load_end = SYSUTCDATETIME(), rows_inserted = @rows_ins,
            rows_expired = @rows_upd, status = 'SUCCESS'
        WHERE log_id = @log_id;

        PRINT 'load_dim_products_scd — inserted: ' + CAST(@rows_ins AS NVARCHAR)
            + '  expired: ' + CAST(@rows_upd AS NVARCHAR);
    END TRY
    BEGIN CATCH
        UPDATE gold.dw_load_log
        SET load_end = SYSUTCDATETIME(), status = 'FAILED',
            error_message = ERROR_MESSAGE()
        WHERE log_id = @log_id;

        THROW;
    END CATCH
END;
GO


-- ------------------------------------------------------------
-- 3d. load_fact_sales
--     Source: silver.crm_sales_details
--     Resolves FKs to CURRENT dimension members.
--     Skips rows that can't resolve product or customer
--     (logged as orphan count in error_message).
--     Idempotent: MERGE on order_number.
-- ------------------------------------------------------------
IF OBJECT_ID('gold.load_fact_sales', 'P') IS NOT NULL
    DROP PROCEDURE gold.load_fact_sales;
GO

CREATE PROCEDURE gold.load_fact_sales
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @log_id        INT;
    DECLARE @load_start    DATETIME2 = SYSUTCDATETIME();
    DECLARE @rows_ins      INT = 0;
    DECLARE @rows_upd      INT = 0;
    DECLARE @orphan_count  INT = 0;

    INSERT INTO gold.dw_load_log (procedure_name, load_start, status)
    VALUES ('gold.load_fact_sales', @load_start, 'RUNNING');
    SET @log_id = SCOPE_IDENTITY();

    BEGIN TRY
        -- ── Stage: resolve dimension keys ────────────────────────────────
        IF OBJECT_ID('tempdb..#stg_sales') IS NOT NULL DROP TABLE #stg_sales;

        SELECT
            sd.sls_ord_num                          AS order_number,
            pr.product_key,
            cu.customer_key,
            COALESCE(sd.sls_order_dt, sd.sls_ship_dt, sd.sls_due_dt) AS date_key,
            sd.sls_order_dt                         AS order_date,
            sd.sls_ship_dt                          AS shipping_date,
            sd.sls_due_dt                           AS due_date,
            sd.sls_sales                            AS sales_amount,
            sd.sls_quantity                         AS quantity,
            sd.sls_price                            AS price
        INTO #stg_sales
        FROM silver.crm_sales_details sd
        -- Resolve to current dim members only
        LEFT JOIN gold.dim_products  pr
            ON  sd.sls_prd_key  = pr.product_number
            AND pr.scd_is_current = 1
        LEFT JOIN gold.dim_customers cu
            ON  sd.sls_cust_id  = cu.customer_id
            AND cu.scd_is_current = 1;

        -- Count and log orphans (unresolvable FKs) — don't fail the load
        SELECT @orphan_count = COUNT(*)
        FROM   #stg_sales
        WHERE  product_key  IS NULL
            OR customer_key IS NULL
            OR date_key     IS NULL;

        -- Remove orphan rows so FK constraints aren't violated
        DELETE FROM #stg_sales
        WHERE  product_key  IS NULL
            OR customer_key IS NULL
            OR date_key     IS NULL;

        -- ── Ensure every date_key exists in dim_date (auto-backfill) ─────
        INSERT INTO gold.dim_date
               (date_key, year, quarter, month_number, month_name,
                week_number, day_of_week, day_name, is_weekend, is_holiday)
        SELECT DISTINCT
               s.date_key,
               YEAR(s.date_key),
               DATEPART(QUARTER, s.date_key),
               MONTH(s.date_key),
               DATENAME(MONTH, s.date_key),
               DATEPART(WEEK, s.date_key),
               DATEPART(WEEKDAY, s.date_key),
               DATENAME(WEEKDAY, s.date_key),
               CASE WHEN DATEPART(WEEKDAY, s.date_key) IN (1,7) THEN 1 ELSE 0 END,
               0
        FROM   #stg_sales s
        WHERE  NOT EXISTS (
                   SELECT 1 FROM gold.dim_date d WHERE d.date_key = s.date_key
               );

        -- ── MERGE into fact_sales (idempotent) ───────────────────────────
        MERGE gold.fact_sales AS tgt
        USING #stg_sales AS src
            ON tgt.order_number = src.order_number
        WHEN MATCHED AND (
            ISNULL(tgt.sales_amount, -1)  <> ISNULL(src.sales_amount, -1) OR
            ISNULL(tgt.quantity, -1)      <> ISNULL(src.quantity, -1)     OR
            ISNULL(tgt.price, -1)         <> ISNULL(src.price, -1)
        ) THEN
            UPDATE SET
                tgt.product_key    = src.product_key,
                tgt.customer_key   = src.customer_key,
                tgt.date_key       = src.date_key,
                tgt.order_date     = src.order_date,
                tgt.shipping_date  = src.shipping_date,
                tgt.due_date       = src.due_date,
                tgt.sales_amount   = src.sales_amount,
                tgt.quantity       = src.quantity,
                tgt.price          = src.price
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (order_number, product_key, customer_key, date_key,
                    order_date, shipping_date, due_date,
                    sales_amount, quantity, price, dw_insert_date)
            VALUES (src.order_number, src.product_key, src.customer_key, src.date_key,
                    src.order_date, src.shipping_date, src.due_date,
                    src.sales_amount, src.quantity, src.price, SYSUTCDATETIME());

        -- @@ROWCOUNT after MERGE = total affected rows; split by action:
        SET @rows_ins = (SELECT COUNT(*) FROM #stg_sales);  -- approx; refine with OUTPUT if needed

        UPDATE gold.dw_load_log
        SET load_end      = SYSUTCDATETIME(),
            rows_inserted = @rows_ins,
            status        = 'SUCCESS',
            error_message = CASE WHEN @orphan_count > 0
                                 THEN 'Orphan rows skipped: ' + CAST(@orphan_count AS NVARCHAR)
                                 ELSE NULL END
        WHERE log_id = @log_id;

        PRINT 'load_fact_sales — processed: ' + CAST(@rows_ins AS NVARCHAR)
            + '  orphans skipped: ' + CAST(@orphan_count AS NVARCHAR);
    END TRY
    BEGIN CATCH
        UPDATE gold.dw_load_log
        SET load_end = SYSUTCDATETIME(), status = 'FAILED',
            error_message = ERROR_MESSAGE()
        WHERE log_id = @log_id;

        THROW;
    END CATCH
END;
GO


-- ============================================================
-- SECTION 4: Master Load Orchestration
--   Call this once per ETL cycle.
--   Order matters: dates → dims → fact
-- ============================================================
IF OBJECT_ID('gold.load_all', 'P') IS NOT NULL DROP PROCEDURE gold.load_all;
GO

CREATE PROCEDURE gold.load_all
AS
BEGIN
    SET NOCOUNT ON;
    EXEC gold.load_dim_date;               -- extend range via params as needed
    EXEC gold.load_dim_customers_scd;
    EXEC gold.load_dim_products_scd;
    EXEC gold.load_fact_sales;
END;
GO
