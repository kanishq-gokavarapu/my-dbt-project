# Databricks notebook source
from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local").appName("csv cleaning").getOrCreate()
print(spark)




# COMMAND ----------

inp_df = spark.read.options(header=True, inferschema=True, delimiter = ',').csv("/FileStore/tables/Amazon_Sale_Report.csv")
inp_df.printSchema()

# COMMAND ----------

## To remove row level duplicates and drop unnamed column

from pyspark.sql.functions import *
unique_df = inp_df.dropDuplicates().drop(col("Unnamed: 22"))
#unique_df.printSchema()

# COMMAND ----------

# To remove unwanted characters from ship-state which includes numbers and any symbols
uniqdf_cleanedcity = unique_df.withColumn("foreign_characters", trim(upper(regexp_replace(col("ship-city"), "[^a-zA-Z ]", "")))).drop("ship-city")
#uniqdf_cleanedcity.select("order id", "updated_city").where(col("order id")=='403-9025939-2809927').show()
# To fill constant values for the ones where we are seeing missing values for ship-city, state, postal code and country
#updated_postaldf = uniqdf_cleanedcity.fillna({"updated_city":"UNKNOWN", "ship-state":"UNKNOWN", "ship-postal-code":999999,"ship-country":"UNKNOWN"})



#updated_df.display()


# COMMAND ----------

updated_postaldf= uniqdf_cleanedcity.withColumn("updated_city", when (col("foreign_characters") == "", "UNKNOWN").otherwise(col("foreign_characters")))
updated_df = updated_postaldf.withColumnRenamed("updated_city", "ship-city").drop(col("foreign_characters"))
updated_df.persist()

# COMMAND ----------


from pyspark.sql import functions as F

Enrich_courier_status_df = updated_df.withColumn(
    "modified_execution_status",
    F.when((F.col("status") == 'Cancelled') & 
            (F.col("courier status").isNull()) & 
            (F.col("qty") == 0), "Cancelled")
     .when((F.col("status").contains("Shipped")) & 
           (F.col("courier status").isNull()) & 
           (F.col("qty") == 0), "UNKNOWN")
     .otherwise(F.col("courier status"))
).drop(F.col("courier status"))
updateddf_courier =Enrich_courier_status_df.withColumnRenamed("modified_execution_status", "courier status")

updateddf_courier.persist()
updated_df.unpersist()


# COMMAND ----------

#handling nulls across other columns
final_dataframe = updateddf_courier.fillna({"qty":0, "currency":"INR","Amount":0, "promotion-ids":"Not Applicable", 
                                            "fulfilled-by":"Not Applicable","ship-city":"UNKNOWN", "ship-state":"UNKNOWN", "ship-postal-code":999999,"ship-country":"UNKNOWN"})

final_dataframe.select("index", "Order ID", date_format("Date", "yyyy-MM-dd").alias("order_date"), "Status" , 	"Fulfilment", "Sales Channel ",  "ship-service-level", "Style", "SKU", 	"Category", "Size", "ASIN", "Courier Status", "Qty", "currency", "Amount", "ship-city","ship-state", "ship-postal-code","ship-country",	"promotion-ids", "B2B", "fulfilled-by").orderBy("index").repartition(1).write.mode("overwrite").format("csv").save("C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Data_cleansed_report.csv")

