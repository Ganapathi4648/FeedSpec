from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, regexp_replace, when, round, upper, lit

spark = SparkSession.builder.appName("CreateNewDataset").getOrCreate()


df = spark.read.option("header", "true").csv("orders_20230701.csv")
print(df.show())

df_transformed = (
    df
    .withColumn("order_key", regexp_replace(col("order_id"), " ", ""))
    .withColumn("customer_fk", col("cust_id").cast("int"))
    .withColumn("order_date", to_date(col("order_date"), "MM-dd-yyyy")) 
    .withColumn("unit_price", round(
        when(col("price") == "", 0.0).otherwise(col("price").cast("float")), 2))
    .withColumn("sku", upper(regexp_replace(col("product_code"), "-", "")))
    .withColumn("discount_price", col("unit_price") * lit(0.9))
    
    .select("order_key", "customer_fk", "order_date", "unit_price", "sku", "discount_price")
)
print(df_transformed.show())
