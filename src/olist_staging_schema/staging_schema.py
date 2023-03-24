import logging
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s :: %(levelname)-8s :: %(name)s :: %(message)s",
)
logger = logging.getLogger("data_warehouse_schema")


class StagingSchema:
    def __init__(self, db_host, db_port, db_name, username, password):
        self.conn = psycopg2.connect(
            host=db_host, port=db_port, dbname=db_name, user=username, password=password
        )
        self.cursor = self.conn.cursor()

    def ingest_olist_files(self, file_paths: list, files_dir_path: str):
        """Ingest the Olist data files into the tables of the staging schema.

        Args:
          file_paths (list[str]): List of CSV file names to ingest.
          files_dir_path (str): Absolute path to the folder where the files to ingest are.
        """
        import os

        logger.info(
            "ingest_into_stagging_tables :: Populating the 'staging' schema tables"
        )

        for file_path in file_paths:
            file_name = file_path.split("/")[-1]
            if file_name != "product_category_name_translation.csv":
                table = file_name[6:-12]
            else:
                table = file_name[:-4]
            self.cursor.execute(
                f"""
                COPY staging.{table} FROM '{os.path.join(files_dir_path, file_path)}' USING DELIMITERS ',' CSV HEADER;
            """
            )
            self.conn.commit()

    def init(self):
        self.create_staging_schema()
        self.create_staging_tables()

    def create_staging_schema(self):
        """Creates the staging schema on the database."""
        logger.info(
            "create_staging_schema :: Creating the 'staging' schema if not exists"
        )

        schema_name = "staging"
        self.cursor.execute(f"""CREATE SCHEMA IF NOT EXISTS {schema_name}""")
        self.conn.commit()

    def create_staging_tables(self):
        """Creates the staging schema tables if they don't exist."""
        logger.info(
            "create_staging_tables :: Creating the 'staging' schema' tables if not exists"
        )

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS staging.geolocation (
                geolocation_zip_code_prefix CHAR(5),
                geolocation_lat VARCHAR(30) NOT NULL,
                geolocation_lng VARCHAR(30) NOT NULL,
                geolocation_city VARCHAR(40) NOT NULL,
                geolocation_state VARCHAR(2) NOT NULL
            )
        """
        )

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS staging.customers (
                customer_id CHAR(32) PRIMARY KEY,
                customer_unique_id CHAR(32),
                customer_zip_code_prefix CHAR(5) NOT NULL,
                customer_city VARCHAR(40) NOT NULL,
                customer_state VARCHAR(2) NOT NULL
            )
        """
        )

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS staging.products (
                product_id CHAR(32) PRIMARY KEY,
                product_category_name VARCHAR(60),
                product_name_lenght NUMERIC(3),
                product_description_lenght NUMERIC(5),
                product_photos_qty NUMERIC(2),
                product_weight_g NUMERIC(6),
                product_length_cm NUMERIC(6),
                product_height_cm NUMERIC(6),
                product_width_cm NUMERIC(6)
            )
        """
        )

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS staging.orders (
                order_id CHAR(32) PRIMARY KEY,
                customer_id CHAR(32) NOT NULL REFERENCES staging.customers (customer_id),
                order_status VARCHAR(20) NOT NULL,
                order_purchase_timestamp TIMESTAMP NOT NULL,
                order_approved_at TIMESTAMP,
                order_delivered_carrier_date TIMESTAMP,
                order_delivered_customer_date TIMESTAMP,
                order_estimated_delivery_date TIMESTAMP
            )
        """
        )

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS staging.sellers(
                seller_id CHAR(32) PRIMARY KEY,
                seller_zip_code_prefix CHAR(5) NOT NULL,
                seller_city VARCHAR(40) NOT NULL,
                seller_state VARCHAR(2) NOT NULL
            )
        """
        )

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS staging.order_items (
                order_id CHAR(32) NOT NULL,
                order_item_id SMALLINT NOT NULL,
                product_id CHAR(32) NOT NULL,
                seller_id CHAR(32) NOT NULL REFERENCES staging.sellers (seller_id),
                shipping_limit_date TIMESTAMP,
                price NUMERIC(12, 2) NOT NULL,
                freight_value NUMERIC(12, 2),
                PRIMARY KEY (order_id, order_item_id)
            )
        """
        )

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS staging.order_payments (
                order_id CHAR(32) REFERENCES staging.orders (order_id),
                payment_sequential NUMERIC(3) NOT NULL,
                payment_type VARCHAR(20) NOT NULL,
                payment_installments NUMERIC(3) NOT NULL,
                payment_value NUMERIC(12, 2) NOT NULL,
                PRIMARY KEY (order_id, payment_sequential)
            )
        """
        )

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS staging.order_reviews (
                review_id CHAR(32),
                order_id CHAR(32) NOT NULL,
                review_score NUMERIC(1) NOT NULL,
                review_comment_title TEXT,
                review_comment_message TEXT,
                review_creation_date TIMESTAMP NOT NULL,
                review_answer_timestamp TIMESTAMP,
                PRIMARY KEY (review_id, order_id)
            )
        """
        )

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS staging.product_category_name_translation(
                product_category_name VARCHAR(60) NOT NULL,
                product_category_name_english VARCHAR(60)
            )
        """
        )

        self.conn.commit()
