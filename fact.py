


def run(spark):

    from pyspark.sql import functions as F
    # Import Config
    from config import get_config
    CATALOG_NAME = get_config("catalog_name", required=True)
    DB_NAME = get_config("db_name", required=True)
        
    # Fallback/Sanity Check
    if not CATALOG_NAME: CATALOG_NAME = "csi_catalog"
    if not DB_NAME: DB_NAME = "sanplan25"
        
    TARGET_NAMESPACE = f"{CATALOG_NAME}.{DB_NAME}"
    print(f"DEBUG: fact.py using TARGET_NAMESPACE: {TARGET_NAMESPACE}")


    dim_part = spark.read.format("iceberg").load(f"{TARGET_NAMESPACE}.dim_part1")
    dim_customer = spark.read.format("iceberg").load(f"{TARGET_NAMESPACE}.dim_customer1")
    dim_po = spark.read.format("iceberg").load(f"{TARGET_NAMESPACE}.dim_po1")
    dim_shipper = spark.read.format("iceberg").load(f"{TARGET_NAMESPACE}.dim_shipper1")
    dim_release = spark.read.format("iceberg").load(f"{TARGET_NAMESPACE}.dim_release")

    # 2. Source Data (Staging/Transactional)


    fact_sales = spark.sql(f"""
        SELECT
            -- --------------------------------------------------------
            -- KEYS: Fetch explicitly from DIMENSIONS (FKs)
            -- --------------------------------------------------------
            
            
            dp_po.po_key,
            dp_part.part_key,
            dc.customer_key AS customer_address_no,
            ds.shipper_key,
            dr.release_key,

            -- MEASURES fetched from Dimension
            dr.Quantity_Ordered,
            dr.Quantity_Shipped,
            (dr.Quantity_Ordered - dr.Quantity_Shipped) AS Quantity_Remaining,
            svp.Price,
            dr.Quantity_Ordered * svp.price AS amount,
            --ds.freight_amount AS Freight_Amount, -- Sourced from Dim Shipper as requested previously

            -- DATES & ATTRIBUTES
            dr.ship_date,
            dr.due_date as customer_due_date,
            p.period_display as Period,
            CONCAT('Q', CAST(p.quarter_group AS STRING)) AS Quarter
 
        FROM Sales_V_PO_Line sal

        -- JOIN DIMENSION TABLE INSTEAD OF SOURCE
        JOIN {TARGET_NAMESPACE}.dim_release dr
            ON sal.PO_Line_Key = TRY_CAST(dr.po_line_key AS INTEGER)

        LEFT JOIN Sales_V_PO sls
            ON sal.PO_Key = TRY_CAST(sls.PO_Key AS INTEGER)

        -- JOIN DIMENSIONS TO GET SURROGATE KEYS
        -- The Fact Table MUST link to the Dimension ID.

        LEFT JOIN Sales_V_Price svp 
        ON svp.po_line_key = sal.PO_Line_Key
        
        LEFT JOIN PlexPeriod p             
        ON dr.ship_date >= p.Begin_Date
       AND dr.ship_date < p.End_Date

        
        -- 1. Dim Part
        -- Join Condition: Source.Part_Key = Dim.part_key (Natural Key Match)
        LEFT JOIN {TARGET_NAMESPACE}.dim_part1 dp_part
            ON TRY_CAST(sal.Part_Key AS INTEGER) = dp_part.part_key

        -- 2. Dim PO
        LEFT JOIN {TARGET_NAMESPACE}.dim_po1 dp_po
            ON TRY_CAST(sal.PO_Key AS INTEGER) = dp_po.po_key

        -- 3. Dim Customer
        LEFT JOIN {TARGET_NAMESPACE}.dim_customer1 dc
            ON sls.Customer_Address_No = dc.customer_key
            
        -- 4. Dim Shipper
        -- (Complex Join Logic as defined by user: Linked via Release Key)
        LEFT JOIN {TARGET_NAMESPACE}.dim_shipper1 ds
            ON TRY_CAST(dr.release_key AS INTEGER) = ds.release_key
        
        ORDER BY dr.ship_date
    """)


    fact_sales.write.format("iceberg") \
        .mode("overwrite") \
        .saveAsTable(f"{TARGET_NAMESPACE}.fact_sales_release12")


