def write_parquet(df,output_location):
   return df.write.format("parquet").mode("overwrite").save(str(output_location))