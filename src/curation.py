from pyspark.sql.functions import count,sum
def aggregate_order_items(df):
    return df.groupBy("order_id").agg(
    count("order_item_id").alias("order_item_count"),
    sum("price").alias("total_item_value"),
    sum("freight_value").alias("total_freight"))

def aggregate_payment(df):
    return df.groupBy("order_id").agg(sum("payment_value").alias("total_payment_value"),count("payment_value").alias("payment_count"))

def build_order_curated(orders_df,order_items_agg_df,payment_agg_df,customer_df):
    curated_df = orders_df.join(order_items_agg_df,on="order_id",how="left")
    curated_df = curated_df.join(payment_agg_df,on="order_id",how="left")
    curated_df = curated_df.join(customer_df,on="customer_id",how="left")
    return curated_df