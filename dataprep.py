import pyspark
from pyspark.sql import SparkSession
import os

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
    print("\n--- Spark DataFrame Statistics ---")
    print(f"Articles total rows:     {articles_df.count():,}")
    print(f"Customers total rows:    {customers_df.count():,}")
    print(f"Transactions total rows: {transactions_df.count():,}")
    
    # 4. Show Schemas to verify context data types
    print("\n--- Transactions Data Schema ---")
    transactions_df.printSchema()
    articles_df.printSchema() 
    customers_df.printSchema()
       # 5. Display a sample of the transactions dataframe
    print("\n--- Previewing first 5 rows of all csv files ---")
    transactions_df.show(5)
    customers_df.show(5)
    articles_df.show(5)
    
    return articles_df, customers_df, transactions_df

if __name__ == "__main__":
    # Start the PySpark session
    spark_session = init_spark()
    
    # Execute the load function
    articles, customers, transactions = load_data_with_pyspark(spark_session)



spark_session.stop()

print("✅ Spark Session Closed")

