import logging
import psycopg2


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s :: %(levelname)-8s :: %(name)s :: %(message)s",
)
logger = logging.getLogger("data_warehouse_schema")


class DataWarehouseSchema:
    def __init__(self, db_host, db_port, db_name, username, password):
        self.conn = psycopg2.connect(
            host=db_host, port=db_port, dbname=db_name, user=username, password=password
        )
        self.cursor = self.conn.cursor()

    def populate_dw_tables(self):
        """Populates the dw schema on the database with data from the staging schema"""
        logger.info("populate_dw_tables :: Populating the 'dw' schema tables")

        self.cursor.execute(
            """
            INSERT INTO dw.dim_customers (
                SELECT
                    customer_id, customer_unique_id, customer_zip_code_prefix
                FROM
                    staging.customers
            );
        """
        )

        self.cursor.execute(
            """
            INSERT INTO dw.orders_fact (
                SELECT
                    o.order_id, o.customer_id, op.payment_value
                FROM
                    staging.orders as o
                INNER JOIN
                    staging.order_payments AS op
                    ON o.order_id = op.order_id
            );
        """
        )

        self.cursor.execute(
            """
            INSERT INTO dw.dim_payments (
                SELECT
                    order_id, payment_installments, payment_type
                FROM
                    staging.order_payments
            );
        """
        )

        self.cursor.execute(
            """
            INSERT INTO dw.dim_order_items (
                SELECT
                    o.order_id, o.product_id, p.product_category_name, o.order_item_id, o.price, o.freight_value
                FROM
                    staging.order_items AS o
                INNER JOIN
                    staging.products AS p
                    ON o.product_id = p.product_id
            );
        """
        )

        self.cursor.execute(
            """
            INSERT INTO dw.dim_geolocation (
                SELECT
                    geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state
                FROM
                    staging.geolocation
            );
        """
        )

        self.cursor.execute(
            """
            INSERT INTO dw.dim_date (
                SELECT
                    order_id, order_status, order_purchase_timestamp, order_delivered_customer_date, order_estimated_delivery_date
                FROM
                    staging.orders
            );
        """
        )

        self.cursor.execute(
            """
            INSERT INTO dw.dim_date (
                SELECT
                    order_id, order_status, order_purchase_timestamp, order_delivered_customer_date, order_estimated_delivery_date
                FROM
                    staging.orders
            );
        """
        )

        self.cursor.execute(
            """
            INSERT INTO dw.dim_review (
                SELECT
                    order_id, payment_installments
                FROM
                    staging.order_payments
            );
        """
        )
        self.conn.commit()

    def init(self):
        self.create_dw_schema()
        self.create_dw_tables()

    def create_dw_schema(self):
        """Creates the dw schema on the database."""
        logger.info("create_dw_schema :: Creating the 'dw' schema if not exists")

        schema_name = "dw"
        self.cursor.execute(f"""CREATE SCHEMA IF NOT EXISTS {schema_name}""")

    def create_dw_tables(self):
        """Creates the dw schema tables if they don't exist."""
        logger.info(
            "create_dw_tables :: Creating the 'dw' schema' tables if not exists"
        )

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS dw.dim_customers (
                customer_id VARCHAR(32) PRIMARY KEY,
                customer_unique_id VARCHAR(32),
                customer_zip_code_prefix VARCHAR(5) NOT NULL
            )
        """
        )

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS dw.orders_fact (
                order_id VARCHAR(32),
                customer_id VARCHAR(32) REFERENCES dw.dim_customers (customer_id),
                payment_value NUMERIC(12, 2) CHECK (payment_value >= 0) NOT NULL
            )
        """
        )

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS dw.dim_payments (
                order_id VARCHAR(32),
                payment_installments VARCHAR(3) NOT NULL,
                payment_type VARCHAR(20) NOT NULL
            )
        """
        )

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS dw.dim_geolocation (
                geolocation_zip_code_prefix VARCHAR(5),
                geolocation_lat VARCHAR(30) NOT NULL,
                geolocation_lng VARCHAR(30) NOT NULL,
                geolocation_city VARCHAR(40) NOT NULL,
                geolocation_state VARCHAR(2)
            )
        """
        )

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS dw.dim_order_items (
                order_id VARCHAR(32),
                product_id VARCHAR(32) NOT NULL,
                product_category_name VARCHAR(60),
                order_item_id VARCHAR(2),
                price NUMERIC(12, 2) CHECK (price > 0) NOT NULL,
                freight_value NUMERIC(12, 2) CHECK (freight_value >= 0) NOT NULL,
                PRIMARY KEY (order_id, order_item_id)
            )
        """
        )

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS dw.dim_date (
                order_id VARCHAR(32),
                order_status VARCHAR(20) NOT NULL,
                order_purchase_timestamp TIMESTAMP NOT NULL,
                order_delivered_customer_date TIMESTAMP,
                order_estimated_delivery_date TIMESTAMP
            )
        """
        )

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS dw.dim_review (
                order_id VARCHAR(32),
                order_review_score NUMERIC(2)
            )
        """
        )

        self.conn.commit()
