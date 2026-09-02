from pyspark.sql.functions import col,when,sum

def  null_checks(df):
    null_expressions=[]
    for column_name in df.columns:
        null_check = (sum(when(col(column_name).isNull(), 1).otherwise(0)).alias(f"{column_name}_null_count"))
        null_expressions.append(null_check)
    return df.agg(*null_expressions)


def duplicate_checks(df,key_column):
    return df.groupBy(key_column).count().filter(col("count") > 1)

def date_validation(df,earlier_column,later_column):
    return df.filter(col(later_column)<col(earlier_column))

def valid_values_check(df, column_name, valid_values):
    return df.filter(~col(column_name).isin(valid_values))

def referential_integrity_check(child_df, parent_df, key_column):
    return child_df.join(parent_df,on=key_column,how="left_anti")
