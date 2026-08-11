import pyspark
from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import col, sum,when,count,min,max,avg
import sys

# 1. TELL PYSPARK EXACTLY WHERE TO FIND YOUR CLEAN JAVA 11 INSTALLATION


# Update this path to match your actual installation folder
os.environ["JAVA_HOME"] = r"C:\\java\\OpenJDK17U-jdk_x64_windows_hotspot_17.0.20_8\\jdk-17.0.20+8"

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
    # print("\n--- Spark DataFrame Statistics ---")
    # print(f"Articles total rows:     {articles_df.count():,}")
    # print(f"Customers total rows:    {customers_df.count():,}")
    # print(f"Transactions total rows: {transactions_df.count():,}")
    
    # 4. Show Schemas to verify context data types
    # print("\n--- Transactions Data Schema ---")
    # transactions_df.printSchema()
    # articles_df.printSchema() 
    # customers_df.printSchema()
       # 5. Display a sample of the transactions dataframe
    # print("\n--- Previewing first 5 rows of all csv files ---")
    # transactions_df.show(5)
    # customers_df.show(5)
    # articles_df.show(5)
    
    return articles_df, customers_df, transactions_df

if __name__ == "__main__":
    # Start the PySpark session
    spark_session = init_spark()
    
    # Execute the load function
    articles, customers, transactions = load_data_with_pyspark(spark_session)



# null_counts_txn = transactions.select([sum(col(c).isNull().cast("int")).alias(c) for c in transactions.columns])
# null_counts_txn.show()
# null_counts_articles = articles.select([sum(col(c).isNull().cast("int")).alias(c) for c in articles.columns])
# null_counts_articles.show()
# null_counts_customers = customers.select([sum(col(c).isNull().cast("int")).alias(c) for c in customers.columns])
# null_counts_customers.show()

#Filling null values in detail description column in article dataframe with "No Description Available"
# articles = articles.fillna({"detail_desc": "No Description Available"})
# #Dropping duplicates in the articles dataframe
# cleaned_df_articles = articles.distinct()
# print("✅ Data Cleaning Completed for Articles DataFrame")
# print(f"✅ Cleaned Articles DataFrame has {cleaned_df_articles.count():,} rows and {len(cleaned_df_articles.columns)} columns.")

# null_counts_articles = articles.select([sum(col(c).isNull().cast("int")).alias(c) for c in articles.columns])
# null_counts_articles.show()

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

# print(duplicate_transactions.count())
cleaned_df_transactions = transactions.dropDuplicates(
    ["customer_id", "article_id", "t_dat"]
)
# print(cleaned_df_transactions.count())
# cleaned_df_transactions.groupBy("sales_channel_id").count().show()
transactions.select(
    min("price").alias("min_price"),
    max("price").alias("max_price"),
    avg("price").alias("avg_price")
).show()
#Filling null values in Active column and FN column based on fashion_news_frequency
# customers = customers.withColumn(
#     "Active",
#     when(
#         col("club_member_status") == "ACTIVE",
#         1.0
#     ).otherwise(0.0)
# )


# customers = customers.withColumn(
#     "FN",
#     when(
#         (col("FN").isNull()) &
#         (col("fashion_news_frequency").isin("Regularly", "Monthly")),
#         1.0
#     ).otherwise(0.0))
# customers.show(5)

# #Checking the value counts in the fashion_news_frequency column


# #fILIING NULL,nONE VALUES IN THE fashion_news_frequency COLUMN AS "None" on the whole dataset
# customers = customers.withColumn(
#     "fashion_news_frequency",
#     when(
#         col("fashion_news_frequency").isNull() |
#         (col("fashion_news_frequency") == "NONE"),
#         "None"
#     ).otherwise(col("fashion_news_frequency")))

#Filling null values in the club_member_status column as "Unknown" on the whole dataset
# customers=customers.withColumn(
#     "club_member_status",
#     when(
#         col("club_member_status").isNull(),
#         "Unknown"
#     ).otherwise(col("club_member_status")))
#Checking and Filling  null values in the age column as -1 on the whole dataset
# customers.select(
#     sum(col("age").isNull().cast("int")).alias("age_nulls")
# ).show()
# total = customers.count()

# customers.select(
#     (sum(col("age").isNull().cast("int")) / total * 100)
#     .alias("age_missing_percentage")
# ).show()
# median_age = customers.approxQuantile(
#     "age",
#     [0.5],
#     0.01
# )[0]

# print(median_age)
# customers = customers.fillna(
#     {"age": median_age})
# customers = customers.withColumn(
#     "age_group",
#     when(col("age") < 25, "Young")
#     .when(col("age") < 40, "Adult")
#     .when(col("age") < 60, "Middle_Aged")
#     .otherwise("Senior")
# customers=customers.approxQuantile("age",[0.0,0.5,1.0],0.01)
# min_age = customers[0]
# max_age = customers[2]
# median_age = customers[1]
# print(f"Min Age: {min_age}, Median Age: {median_age}, Max Age: {max_age}")

#Dropping duplicates in the customers dataframe
# cleaned_df_customers = customers.distinct()
# # 
# # print("✅ Data Cleaning Completed")
# print(f"✅ Cleaned Customers DataFrame has {cleaned_df_customers.count():,} rows and {len(cleaned_df_customers.columns)} columns.")

# null_counts_customers = cleaned_df_customers.select(
#     [
#         sum(col(c).isNull().cast("int")).alias(c)
#         for c in cleaned_df_customers.columns
#     ]
# )

# null_counts_customers.show()
# customers.groupBy("fashion_news_frequency").count().show()
# customers.groupBy("Active").count().show()
# customers.groupBy("FN").count().show()
# customers.groupBy("club_member_status").count().show()
# customers.groupBy("age").count().show()


spark_session.stop()

print("✅ Spark Session Closed")

