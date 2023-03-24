import os
import logging
from src.olist_staging_schema.staging_schema import StagingSchema
from src.olist_dw_schema.data_warehouse_schema import DataWarehouseSchema


def main(logger: logging.Logger):
    _DB_HOST = "database"
    _DB_PORT = "5432"
    _DB_NAME = "olist"
    _DB_USER = "username"
    _DB_PASSWORD = "password"

    # Staging schema
    logger.info("root :: Starting process of the schema: stagging")
    staging_schema = StagingSchema(_DB_HOST, _DB_PORT, _DB_NAME, _DB_USER, _DB_PASSWORD)
    staging_schema.init()
    _FILES_FOLDER = "/olist_dw/data/olist_datasets/"
    _FILE_NAMES_IN_ODER = [  # Need to be in the right order because of the Foreign Keys
        "olist_customers_dataset.csv",
        "olist_orders_dataset.csv",
        "olist_sellers_dataset.csv",
        "olist_geolocation_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_order_payments_dataset.csv",
        "olist_order_reviews_dataset.csv",
        "olist_products_dataset.csv",
        "product_category_name_translation.csv",
    ]
    staging_schema.ingest_olist_files(_FILE_NAMES_IN_ODER, _FILES_FOLDER)

    # Data Warehouse schema
    logger.info("root :: Starting process of the schema: dw")
    dw_schema = DataWarehouseSchema(
        _DB_HOST, _DB_PORT, _DB_NAME, _DB_USER, _DB_PASSWORD
    )
    dw_schema.init()
    dw_schema.populate_dw_tables()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s :: %(levelname)-8s :: %(name)s :: %(message)s",
    )
    logger = logging.getLogger("root")
    main(logger)
