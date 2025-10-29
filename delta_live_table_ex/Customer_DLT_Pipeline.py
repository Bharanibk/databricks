import dlt
from pyspark.sql.functions import *

@dlt.table(
    comment="Raw customer data ingested from CSV"
)
def customers_bronze():
    return spark.read.format("csv").option("header", "true").load("/Workspace/Users/bkbplano@gmail.com/databricks/delta_live_table_ex/customers.csv")


@dlt.table(
    comment="Cleaned customer data with valid emails only"
)
@dlt.expect_or_drop("valid_email", "email IS NOT NULL AND email != ''")
def customers_silver():
    return (
        dlt.read("customers_bronze")
        .withColumn("signup_date", to_date(col("signup_date")))
    )


@dlt.table(
    comment="Customer count by country"
)
def customers_by_country():
    return (
        dlt.read("customers_silver")
        .groupBy("country")
        .agg(count("*").alias("customer_count"))
        .orderBy(desc("customer_count"))
    )