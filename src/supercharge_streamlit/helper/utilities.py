from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.catalog import Catalog


__copyright__  = "Copyright (c) 2024 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


def catalog_exist(tbl_env: StreamExecutionEnvironment, catalog_to_check: str) -> bool:
    """This method checks if the catalog exist in the environment.

    Args:
        tbl_env (StreamExecutionEnvironment): The StreamExecutionEnvironment is the context
        in which a streaming program is executed. 
        catalog_to_check (str): The name of the catalog to be checked if its name exist in the
        environment.

    Returns:
        bool: True is returned, if the catalog exist in the environment.  Otherwise, False is
        returned.
    """
    catalogs = tbl_env.list_catalogs()

    # Check if a specific catalog exists
    if catalog_to_check in catalogs:
        return True
    else:
        return False
    

def load_catalog(tbl_env: StreamExecutionEnvironment, region_name: str, bucket_name: str, catalog_name: str) -> Catalog:
    """ This method loads the catalog into the environment.
    
    Args:
        tbl_env (StreamExecutionEnvironment): The StreamExecutionEnvironment is the context
        in which a streaming program is executed. 
        region_name (str): The region where the bucket is located.
        bucket_name (str): The name of the bucket where the warehouse is located.
        catalog_name (str): The name of the catalog to be loaded into the environment.
        
    Returns:
        Catalog: The catalog object is returned if the catalog is loaded into the environment.
    """
    try:
        if not catalog_exist(tbl_env, catalog_name):
            tbl_env.execute_sql(f"""
                CREATE CATALOG {catalog_name} WITH (
                    'type' = 'iceberg',
                    'warehouse' = 's3://{bucket_name}/warehouse',
                    'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
                    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
                    'glue.skip-archive' = 'True',
                    'glue.region' = '{region_name}'
                    );
            """)
        else:
            print(f"The {catalog_name} catalog already exists.")
    except Exception as e:
        print(f"A critical error occurred to during the processing of the catalog because {e}")
        exit(1)

    # --- Use the Iceberg catalog
    tbl_env.use_catalog(catalog_name)

    # --- Access the Iceberg catalog to query the airlines database
    return tbl_env.get_catalog(catalog_name)


def load_database(tbl_env: StreamExecutionEnvironment, catalog: Catalog, database_name:str) -> None:
    """This method loads the database into the environment.

    Args:
        tbl_env (StreamExecutionEnvironment): The StreamExecutionEnvironment is the con.text
        catalog (Catalog): The catalog object is the catalog to be used to create the database.
        database_name (str): The name of the database to be loaded into the environment.
    """
    try:
        if not catalog.database_exists(database_name):
            tbl_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {database_name};")
        else:
            print(f"The {database_name} database already exists.")
        tbl_env.use_database(database_name)
    except Exception as e:
        print(f"A critical error occurred to during the processing of the database because {e}")
        exit(1)