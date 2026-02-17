


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


    dim_part = spark.read.format("iceberg").load(f"{TARGET_NAMESPACE}.dim_part3")
    dim_customer = spark.read.format("iceberg").load(f"{TARGET_NAMESPACE}.dim_customer11")
    dim_po = spark.read.format("iceberg").load(f"{TARGET_NAMESPACE}.dim_po2")
    dim_shipper = spark.read.format("iceberg").load(f"{TARGET_NAMESPACE}.dim_shipper1")

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
            rel.release_key,

            -- MEASURES from Source
            CAST(rel.Quantity AS DOUBLE) AS Quantity_Ordered,
            CAST(rel.Quantity_Shipped AS DOUBLE) AS Quantity_Shipped,
            (CAST(rel.Quantity AS DOUBLE) - CAST(rel.Quantity_Shipped AS DOUBLE)) AS Quantity_Remaining,
            ds.freight_amount AS Freight_Amount, -- Sourced from Dim Shipper as requested previously

            -- DATES & ATTRIBUTES
            rel.Ship_Date,
            rel.Due_Date,
            sls.Note,
            dp_po.PO_Status -- Denormalized from Dim PO
            
        FROM Sales_V_PO_Line sal

        -- JOIN SOURCE TABLES TO BUILD "STAGING" RECORD
        -- (This reconstructs the transaction event)
        JOIN Sales_V_Release rel
            ON sal.PO_Line_Key = TRY_CAST(rel.po_line_key AS INTEGER)

        LEFT JOIN Sales_V_PO sls
            ON sal.PO_Key = TRY_CAST(sls.PO_Key AS INTEGER)

        -- JOIN DIMENSIONS TO GET SURROGATE KEYS
        -- The Fact Table MUST link to the Dimension ID.
        
        -- 1. Dim Part
        -- Join Condition: Source.Part_Key = Dim.part_key (Natural Key Match)
        LEFT JOIN {TARGET_NAMESPACE}.dim_part dp_part
            ON TRY_CAST(sal.Part_Key AS INTEGER) = dp_part.part_key

        -- 2. Dim PO
        LEFT JOIN {TARGET_NAMESPACE}.dim_po dp_po
            ON TRY_CAST(sal.PO_Key AS INTEGER) = dp_po.po_key

        -- 3. Dim Customer
        LEFT JOIN {TARGET_NAMESPACE}.dim_customer dc
            ON sls.Customer_Address_No = dc.customer_key
            
        -- 4. Dim Shipper
        -- (Complex Join Logic as defined by user: Linked via Release Key)
        LEFT JOIN {TARGET_NAMESPACE}.dim_shipper ds
            ON TRY_CAST(rel.Release_Key AS INTEGER) = ds.release_key

        WHERE rel.Ship_Date > '2025-06-29'
        
        ORDER BY rel.Ship_Date
    """)


    fact_sales.write.format("iceberg") \
        .mode("overwrite") \
        .saveAsTable(f"{TARGET_NAMESPACE}.fact_sales_release1")


