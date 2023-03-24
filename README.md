# Olist Data Warehouse
## OBJECTIVE:
Create a Data Warehouse in the dimensional model (fact and dimensional tables) with data from the [Olist](https://olist.com/) brazilian ecommerce to answer the following questions requested by an imaginary CEO:
1. Average of amounts paid in each payment method.
2. Avarage quantity of customers on the São Paulo and Goiás states from Brazil.
3. Median of the stars review of each product category.
4. Average price of purchase installments in installments.
5. Avarage price of the 25% biggest purchases prices.
6. Quantity of times that a order was delivered after the expected delivery date.
7. Avarage price of each product category.
8. Day of the week with biggest purchases.

## The dataset sources:
The datasets are from [Kaggle](https://www.kaggle.com) and can be found [here](https://www.kaggle.com/olistbr/brazilian-ecommerce).
### Dataset schema:
The image image bellow represents the dataset files in CSV format downloaded from Kaggle and their relationships:

![imageDB](https://i.imgur.com/HRhd2Y0.png)

## Data Warehouse schema:
To answer the imaginary CEO questions, the final data warehouse schema will look like bellow. We have the order_fact table as our Fact table and it dimensional tables:

![imageDW](https://i.imgur.com/7BkaiJH.png)

---
## The ETL pipeline:
To create the Data Warehouse schema, we will execute the following pipeline stages:
1. Create a staging schema (named 'staging') and tables on the database for ingesting data from the dataset files.
2. Create the data warehouse schema (named 'dw) and tables on the database.
3. Use the data on the staging schema to populate the data warehouse schema tables.

---
## How to use this project:
#### Requirements: have [Docker](https://www.docker.com) and [Docker Compose](https://docs.docker.com/compose/install/#install-compose) installed on your machine.

1. Create the Docker container with the PostgreSQL service with the command:
```sh
docker compose up -d
```
2. Start the set_up.sh script on the container. This script set up the container environment and start the scripts that do the ETL pipeline:
```sh
docker compose exec postgres-olist-database bash /olist_dw/set_up.sh
```
3. Now the tables from the data warehouse schema are created and populated with data. The PostgreSQL container exposes the port 5434 for connecting any compatible tool for quering the data.
---

### Useful commands:
#### Show all containers running:
```sh
docker ps
```
#### Stop the project container:
```sh
docker compose down
``` 
#### Execute a command inside the container:
```sh
docker exec postgres-olist-database <command>
```
#### Show the container logs:
```sh
docker logs postgres-olist-database
```
