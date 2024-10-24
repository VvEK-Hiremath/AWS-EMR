from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

import sys
import logging


if __name__ == "__main__":
    args = sys.argv[1:]
    bucket = args[0]
    logging.info("bucket " + bucket)

    # Create the Spark Session
    with SparkSession.builder.appName(
        "Fruit and Veg Shop Data Analysis"
    ).getOrCreate() as spark:

        spark.sparkContext.setLogLevel("DEBUG")
        logging.info("Spark Session Created")

        sales_data_path = f"s3://{bucket}/data/sales.json"

        # Read the JSON Sales Data
        sales_df = spark.read.option("multiline", "true").json(sales_data_path)
        sales_df.show()

        # Explode the Transactions
        exploded_df = sales_df.select(
            col("transaction_id"),
            col("store_location"),
            explode(col("items_sold")).alias("item_detail"),
        )
        exploded_df.show()

        # Select relevant columns and aggregate
        aggregated_df = (
            exploded_df.groupBy("item_detail.item")
            .sum("item_detail.quantity")
            .withColumnRenamed("sum(item_detail.quantity AS quantity)", "total_quantity")
        )
        aggregated_df.show()

        # Sort by total_quantity in descending order
        sorted_df = aggregated_df.orderBy(col("total_quantity").desc())

        # Show the result
        sorted_df.show()

        # Upload the Result to the Bucket
        output_path = f"s3://{bucket}/results/sorted"
        sorted_df.write.format("csv").option("header", "true").save(output_path)
