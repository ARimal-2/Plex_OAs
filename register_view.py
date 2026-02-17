import logging
import sys
from pyspark.sql import SparkSession
from config import get_config


CATALOG_NAME = get_config("catalog_name", required=True)
DB_NAME = get_config("db_name", required=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run(spark): 
    """
    Connects to IBM Iceberg catalog (plexodbc_silver) and registers tables as temporary views.
    """
    
    logger.info("----------------------------------------------------------------")
    logger.info(f"Starting Iceberg View Registration")
    logger.info("----------------------------------------------------------------")

    # 1. MAPPING: Expected Name -> Actual/Required Alias
    target_mappings = {
        "common_v_customer_address": "Common_V_Customer_Address",
        "sales_v_po_line": "Sales_V_PO_Line",
        "sales_v_release": "Sales_V_Release",
        "sales_v_po": "Sales_V_PO",
        "sales_v_shipper_line": "Sales_V_Shipper_Line",
        "sales_v_shipper": "Sales_V_Shipper",
        "part_v_part": "Part_V_Part",
        "sales_v_po_status": "Sales_V_PO_Status",
        "plexus_control_v_plexus_user": "Plexus_control_v_plexus_user",
        "plexperiod": "PlexPeriod"
    }

    # 2. DEFINITION: Source Namespace
    source_namespace = f"{CATALOG_NAME}.plexodbc_silver"
    logger.info(f"Target Source Namespace: {source_namespace}")

    registered_count = 0
    registered_views = []

    try:
        # List tables in the source namespace
        df_tables = spark.sql(f"SHOW TABLES IN {source_namespace}")
        tables = df_tables.collect()
        logger.info(f"Found {len(tables)} tables in {source_namespace}.")

        for row in tables:
            table_name = row.tableName
            lower_name = table_name.lower()
            
            full_table_path = f"{source_namespace}.{table_name}"
            
            if lower_name in target_mappings:
                try:
                    df_source = spark.read.table(full_table_path)
                    
                    # A. Register original name
                    df_source.createOrReplaceTempView(table_name)
                    
                    # B. Register Alias (if different)
                    alias_name = target_mappings[lower_name]
                    if alias_name != table_name:
                        df_source.createOrReplaceTempView(alias_name)
                        logger.info(f"  Registered {table_name} as {alias_name}")
                        registered_views.append(alias_name)
                    else:
                        logger.info(f"  Registered {table_name}")
                        registered_views.append(table_name)
                        
                    registered_count += 1
                    
                except Exception as e:
                    logger.error(f"  Failed to register {full_table_path}: {e}")

    except Exception as e:
        logger.error(f"Critical Error listing/reading tables in {source_namespace}: {e}")
        raise e

    logger.info("----------------------------------------------------------------")
    logger.info(f"Summary: Registered {registered_count} temp views.")
    logger.info(f"Views: {registered_views}")
    logger.info("----------------------------------------------------------------")
    
    # 3. Validation Check
    critical_tables = ["Sales_V_PO", "Sales_V_PO_Line", "Part_V_Part"]
    missing = []
    
    reg_set = {v for v in registered_views}
    for t in critical_tables:
        if t not in reg_set:
            missing.append(t)
            
    if missing:
        logger.warning(f"WARNING: Critical tables missing from registration: {missing}")
    else:
        logger.info("All critical tables registered successfully.")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ExtractIcebergToViews") \
        .getOrCreate()
    run(spark)
