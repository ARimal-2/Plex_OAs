from pyspark.sql import functions as F
from config import get_config

def run(spark):
    """
    Executes the Dimension table transformations based on User's explicit SQL queries.
    """

    CATALOG_NAME = get_config("catalog_name", required=True)
    DB_NAME = get_config("db_name", required=True)
    
    # Fallback/Sanity Check
    if not CATALOG_NAME: CATALOG_NAME = "csi_catalog"
    if not DB_NAME: DB_NAME = "sanplan25"
        
    TARGET_NAMESPACE = f"{CATALOG_NAME}.{DB_NAME}"
    print(f"DEBUG: dimension.py using TARGET_NAMESPACE: {TARGET_NAMESPACE}")

    # 1. dim_customer
    dim_customer = spark.sql("""
        SELECT DISTINCT
            Customer_Address_No AS customer_key,
            Customer_Address_Code AS ship_to
        FROM Common_V_Customer_Address
    """)
    dim_customer.write.format("iceberg").mode("overwrite").saveAsTable(f"{TARGET_NAMESPACE}.dim_customer11")


    # 2. dim_shipper
    dim_shipper = spark.sql("""
        SELECT
            s.shipper_key,
            s.freight_amount,
            l.release_key
        FROM sales_v_shipper s
        LEFT JOIN sales_v_shipper_line l
            ON TRY_CAST(s.Shipper_Key AS INTEGER) = TRY_CAST(l.Shipper_Key AS INTEGER)
    """)
    dim_shipper.write.format("iceberg").mode("overwrite").saveAsTable(f"{TARGET_NAMESPACE}.dim_shipper1")


    # 3. dim_po
    dim_po = spark.sql("""
        SELECT
            sls.po_key,
            sls.order_no,
            sls.po_date,
            sls.po_no AS customer_po,
            sls.note,
            svps.PO_Status
        FROM sales_v_po sls
        LEFT JOIN Sales_V_PO_Status svps
            ON TRY_CAST(sls.PO_Status_Key AS INTEGER) = svps.PO_Status_Key
    """)
    dim_po.write.format("iceberg").mode("overwrite").saveAsTable(f"{TARGET_NAMESPACE}.dim_po2")


    # 4. dim_part
    dim_part = spark.sql("""
        SELECT 
            part_key,
            part_no,
            name,
            revision,
            CONCAT(Part_No, '-', Revision) AS Item_No
        FROM part_v_part
    """)
    dim_part.write.format("iceberg").mode("overwrite").saveAsTable(f"{TARGET_NAMESPACE}.dim_part3")
