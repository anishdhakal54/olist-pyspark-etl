from pyspark.sql.functions import col,datediff,year,month,when,lit

def add_delivery_days(df):
    return df.withColumn("delivery_days",datediff(start=col("order_purchase_timestamp"),end=col("order_delivered_customer_date")))

def add_date_parts(df, date_column, prefix):
    return df.withColumns({f"{prefix}_year":year(col(date_column)),f"{prefix}_month":month(col(date_column))})

def add_late_delivery_flag(df):
    return df.withColumn("is_late",when(col("order_delivered_customer_date").isNull(),lit(None)).when(col("order_estimated_delivery_date")<col("order_delivered_customer_date"),1).otherwise(0))


 