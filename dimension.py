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
    dim_customer.write.format("iceberg").mode("overwrite").saveAsTable(f"{TARGET_NAMESPACE}.dim_customer1")


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
            sls.po_date AS Order_Date,
            sls.po_no AS customer_po,
            sls.note,
            svps.PO_Status,
            CONCAT(ppu.first_name, ' ', ppu.last_name) AS Inside_Sales,
            ppt.po_type
        FROM sales_v_po sls
        LEFT JOIN Sales_V_PO_Status svps
            ON TRY_CAST(sls.PO_Status_Key AS INTEGER) = svps.PO_Status_Key
        left join Plexus_control_v_plexus_user ppu on sls.inside_sales = ppu.Plexus_user_no
        left join plex_po_type ppt on sls.po_type_key = ppt.po_type_key
    """)
    dim_po.write.format("iceberg").mode("overwrite").saveAsTable(f"{TARGET_NAMESPACE}.dim_po1")


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
    dim_part.write.format("iceberg").mode("overwrite").saveAsTable(f"{TARGET_NAMESPACE}.dim_part1")

    #dim_release
    dim_release = spark.sql("""
        SELECT 
            release_key,
            ship_date,
            due_date,
            po_line_key,
           CAST(Quantity AS DOUBLE) AS Quantity_Ordered,
            CAST(Quantity_Shipped AS DOUBLE) AS Quantity_Shipped 
        FROM sales_v_release
    """)
    dim_release.write.format("iceberg").mode("overwrite").saveAsTable(f"{TARGET_NAMESPACE}.dim_release")

    
