import pyspark
from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import col, sum,when,count,min,max,avg
import sys

# 1. TELL PYSPARK EXACTLY WHERE TO FIND YOUR CLEAN JAVA 11 INSTALLATION


# Update this path to match your actual installation folder
os.environ["JAVA_HOME"] = r"C:\\java\\jdk-17"

# Add the bin folder to the system PATH environment variable
os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]
 # <-- Change this to match your actual install folder path

# 2. FORCE PYSPARK TO USE THE VIRTUAL ENVIRONMENT'S INTERPRETER
# This matches the 'recomm_env' execution framework you are using
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# 3. PERMIT INTERNAL LOCALPORT SWAPPING (Bypasses minor firewall blocks)
os.environ["PYSPARK_ALLOW_INSECURE_GATEWAY"] = "1"



# 1. Define the exact path to your dataset on the D drive
DATA_DIR = r"D:\\Recommendation_system\\data"

def init_spark():
    print("🚀 Initializing PySpark Session...")
    # Configured to use all available local CPU cores and manage memory efficiently
    spark = SparkSession.builder \
        .appName("HM_Context_Aware_Recommender") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "100") \
        .master("local[*]") \
        .getOrCreate()
    return spark

def load_data_with_pyspark(spark):
    print("📊 Loading CSV files via PySpark...")
    
    # 2. Read datasets with schema inference enabled
    # Header=True ensures columns are named correctly; inferSchema parses types
    articles_df = spark.read.csv("D:\\Recommendation_system\\Context-Aware-Neural-Recommendation\\data\\articles.csv", header=True, inferSchema=True)
    customers_df = spark.read.csv("D:\\Recommendation_system\\Context-Aware-Neural-Recommendation\\data\\customers.csv", header=True, inferSchema=True)
    transactions_df = spark.read.csv("D:\Recommendation_system\\Context-Aware-Neural-Recommendation\\data\\transactions_train.csv", header=True, inferSchema=True)
    transactions_df.cache()


    # 3. Print Row Counts (PySpark computes this lazily/efficiently)
    print("\n--- Spark DataFrame Statistics ---")
    print(f"Articles total rows:     {articles_df.count():,}")
    print(f"Customers total rows:    {customers_df.count():,}")
    print(f"Transactions total rows: {transactions_df.count():,}")
    
    # 4. Show Schemas to verify context data types
    # print("\n--- Transactions Data Schema ---")
    transactions_df.printSchema()
    articles_df.printSchema() 
    customers_df.printSchema()
       # 5. Display a sample of the transactions dataframe
    # print("\n--- Previewing first 5 rows of all csv files ---")
    transactions_df.show(5)
    customers_df.show(5)
    articles_df.show(5)
    
    return articles_df, customers_df, transactions_df

if __name__ == "__main__":
    # Start the PySpark session
    spark_session = init_spark()
    
    # Execute the load function
    articles, customers, transactions = load_data_with_pyspark(spark_session)


#Filling null values in detail description column in article dataframe with "No Description Available"
articles = articles.fillna({"detail_desc": "No Description Available"})
#Dropping duplicates in the articles dataframe
cleaned_df_articles = articles.distinct()
print("✅ Data Cleaning Completed for Articles DataFrame")
print(f"✅ Cleaned Articles DataFrame has {cleaned_df_articles.count():,} rows and {len(cleaned_df_articles.columns)} columns.")

null_counts_articles = cleaned_df_articles.select(
    [
        sum(col(c).isNull().cast("int")).alias(c)
        for c in cleaned_df_articles.columns
    ]
)
null_counts_articles.show(5)


#Dropping duplicates in the transactions dataframe
duplicate_transactions = (
    transactions
    .groupBy(
        "customer_id",
        "article_id",
        "t_dat"
    )
    .count()
    .filter("count > 1")
)

#processing date  column in transactions dataframe
from pyspark.sql.functions import (
    year, month, dayofmonth,
    dayofweek, weekofyear, quarter
)

transactions = (
    transactions
    .withColumn("year", year("t_dat"))
    .withColumn("month", month("t_dat"))
    .withColumn("day", dayofmonth("t_dat"))
    .withColumn("day_of_week", dayofweek("t_dat"))
    .withColumn("week_of_year", weekofyear("t_dat"))
    .withColumn("quarter", quarter("t_dat"))
)

#Cleaned transactions dataframe by dropping duplicates.
cleaned_df_transactions = transactions.dropDuplicates(
    ["customer_id", "article_id", "t_dat"]
)


null_counts_transactions = cleaned_df_transactions.select(
    [
        sum(col(c).isNull().cast("int")).alias(c)
        for c in cleaned_df_transactions.columns
    ]
)
print(f"✅ Cleaned Transactions DataFrame has {cleaned_df_transactions.count():,} rows and {len(cleaned_df_transactions.columns)} columns.")
null_counts_transactions.show()

# Filling null values in Active column and FN column based on fashion_news_frequency
customers = customers.withColumn(
    "Active",
    when(
        col("club_member_status") == "ACTIVE",
        1.0
    ).otherwise(0.0)
)

customers = customers.withColumn(
    "FN",
    when(
        (col("FN").isNull()) &
        (col("fashion_news_frequency").isin("Regularly", "Monthly")),
        1.0
    ).otherwise(0.0))
customers.show(5)

#fILIING NULL,nONE VALUES IN THE fashion_news_frequency COLUMN AS "None" on the whole dataset
customers = customers.withColumn(
    "fashion_news_frequency",
    when(
        col("fashion_news_frequency").isNull() |
        (col("fashion_news_frequency") == "NONE"),
        "None"
    ).otherwise(col("fashion_news_frequency")))

# Filling null values in the club_member_status column as "Unknown" on the whole dataset
customers=customers.withColumn(
    "club_member_status",
    when(
        col("club_member_status").isNull(),
        "Unknown"
    ).otherwise(col("club_member_status")))

median_age = customers.approxQuantile(
    "age",
    [0.5],
    0.01
)[0]

# print(median_age)
customers = customers.fillna(
    {"age": median_age})


# Dropping duplicates in the customers dataframe
cleaned_df_customers = customers.distinct()
# 
# print("✅ Data Cleaning Completed")
print(f"✅ Cleaned Customers DataFrame has {cleaned_df_customers.count():,} rows and {len(cleaned_df_customers.columns)} columns.")

null_counts_customers = cleaned_df_customers.select(
    [
        sum(col(c).isNull().cast("int")).alias(c)
        for c in cleaned_df_customers.columns
    ]
)
print(f"✅ Cleaned Customers DataFrame has {cleaned_df_customers.count():,} rows and {len(cleaned_df_customers.columns)} columns.")

null_counts_customers.show()


# ==============================
# SAVE PROCESSED DATA AS PARQUET
# ==============================

PARQUET_DIR = r"D:\\Recommendation_system\\Context-Aware-Neural-Recommendation\\data\\parquet"

os.makedirs(PARQUET_DIR, exist_ok=True)

print("💾 Saving DataFrames as Parquet...")

articles.write \
    .mode("overwrite") \
    .parquet(os.path.join(PARQUET_DIR, "articles"))

customers.write \
    .mode("overwrite") \
    .parquet(os.path.join(PARQUET_DIR, "customers"))

transactions.write \
    .mode("overwrite") \
    .parquet(os.path.join(PARQUET_DIR, "transactions"))

print("✅ Articles saved")
print("✅ Customers saved")
print("✅ Transactions saved")

spark_session.stop()

print("✅ Spark Session Closed")

