# Databricks notebook source
pip install azureml-opendatasets

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.functions import year, month, mean, median



# COMMAND ----------

from azureml.opendatasets import NycTlcYellow

from datetime import datetime
from dateutil import parser


end_date = parser.parse('2018-05-02')
start_date = parser.parse('2018-05-01')
nyc_tlc = NycTlcYellow(start_date=start_date, end_date=end_date)
nyc_tlc_df = nyc_tlc.to_spark_dataframe()

display(nyc_tlc_df.limit(5))

# COMMAND ----------

# MAGIC %md Filter relevant columns

# COMMAND ----------

relevant_df = nyc_tlc_df['vendorID', 'tpepPickupDateTime', 'tpepDropoffDateTime', 'passengerCount', 'rateCodeId', 'paymentType', 'fareAmount','extra', 'mtaTax', 'improvementSurcharge', 'tipAmount','tollsAmount', 'totalAmount']

# COMMAND ----------

# MAGIC %md Drop duplicates

# COMMAND ----------

nyc_tlc_df = relevant_df.drop_duplicates()

# COMMAND ----------

# MAGIC %md Take rows where total amount for a taxi ride is positive

# COMMAND ----------

cleaned_df = nyc_tlc_df .filter(nyc_tlc_df ["totalAmount"] >= 0)


# COMMAND ----------

# MAGIC %md Ensure data type for  columns which wil be used  later for aggregation are numeric

# COMMAND ----------

numerical_columns = [
    "fareAmount",
    "extra",
    "mtaTax",
    "improvementSurcharge",
    "tipAmount",
    "tollsAmount",
    "totalAmount",
]

for column in numerical_columns:
    if column in cleaned_df.columns:  # Check if the column exists in DataFrame
        if cleaned_df.schema[column].dataType not in ["integer", "double"]:
            # Convert data type to double (float) if not already numeric
            cleaned_df = cleaned_df.withColumn(column, col(column).cast("double"))



# COMMAND ----------

# MAGIC %md Add a column : additional_cost which represents the amount beside the actual fare_amount for a taxi ride

# COMMAND ----------

df_cost_column_added = cleaned_df.withColumn("additional_cost", F.col('extra') + F.col("mtaTax")+ F.col('tollsAmount')+ F.col('improvementSurcharge')+F.col('tipAmount'))


# COMMAND ----------

# MAGIC %md Extract year and month from Pickupdate column

# COMMAND ----------

 # Calculate year and month from pickup datetime
df = df_cost_column_added.withColumn("year", year("tpepPickupDateTime")) \
               .withColumn("month", month("tpepPickupDateTime"))

# COMMAND ----------

# MAGIC %md Perform aggregations to calculatee mean, median for passengerCount, totalAmount, additional_cost grouped by paymentType, year, month

# COMMAND ----------

# Group by 'payment_type', 'year', and 'month', and calculate mean and median for 'fare_amount', 'tip_amount', and 'passenger_count'
agg_df = df.groupBy("paymentType", "year", "month") \
           .agg(mean("passengerCount").alias("mean_passengerCount"),
                mean("totalAmount").alias("mean_totalAmount"),
                mean("additional_cost").alias("mean_additional_cost"),
                median("passengerCount").alias("passengerCount"),
                median("totalAmount").alias("median_totalAmount"),
                median("additional_cost").alias("median_additional_cost"))\
           .orderBy("paymentType", "year", "month")

# Show the aggregated DataFrame
agg_df.show()




# COMMAND ----------

# MAGIC %md Write the summarized df to parquet file

# COMMAND ----------

output_path = "/tmp/output_agg_data_parquet"

agg_df.write.mode("overwrite").parquet(output_path)


# COMMAND ----------


