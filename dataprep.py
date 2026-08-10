          # import pyspark
          # from pyspark.sql import SparkSession
          # import os
          
          # import sys
           
          # # Updation with installation folder
          # os.environ["JAVA_HOME"] = r"C:\\java\\OpenJDK17U-jdk_x64_windows_hotspot_17.0.20_8\\jdk-17.0.20+8"
          
          # # Add the bin folder to the system PATH environment variable
          # os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ["PATH"]
          
          # # 2. FORCE PYSPARK TO USE THE VIRTUAL ENVIRONMENT'S INTERPRETER
       
          # os.environ["PYSPARK_PYTHON"] = sys.executable
          # os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
          
          # # 3. PERMIT INTERNAL LOCALPORT SWAPPING (Bypasses minor firewall blocks)
          # os.environ["PYSPARK_ALLOW_INSECURE_GATEWAY"] = "1"
          
          
          
          # # 1. Define the exact path to the dataset on the D drive
          # DATA_DIR = r"D:\\Recommendation_system\\data"
          
          # def init_spark():
          #     print("🚀 Initializing PySpark Session...")
          #     # Configured to use all available local CPU cores and manage memory efficiently
          #     spark = SparkSession.builder \
          #         .appName("HM_Context_Aware_Recommender") \
          #         .config("spark.driver.memory", "8g") \
          #         .config("spark.sql.shuffle.partitions", "100") \
          #         .master("local[*]") \
          #         .getOrCreate()
          #     return spark
          
          # def load_data_with_pyspark(spark):
          #     print("📊 Loading CSV files via PySpark...")
              
          #     # 2. Read datasets with schema inference enabled
          #     # Header=True ensures columns are named correctly; inferSchema parses types
          #     articles_df = spark.read.csv("D:\\Recommendation_system\\Context-Aware-Neural-Recommendation\\data\\articles.csv", header=True, inferSchema=True)
          #     customers_df = spark.read.csv("D:\\Recommendation_system\\Context-Aware-Neural-Recommendation\\data\\customers.csv", header=True, inferSchema=True)
          #     transactions_df = spark.read.csv("D:\Recommendation_system\\Context-Aware-Neural-Recommendation\\data\\transactions_train.csv", header=True, inferSchema=True)
          #     transactions_df.cache()
          
          
          #     # 3. Print Row Counts (PySpark computes this lazily/efficiently)
          #     print("\n--- Spark DataFrame Statistics ---")
          #     print(f"Articles total rows:     {articles_df.count():,}")
          #     print(f"Customers total rows:    {customers_df.count():,}")
          #     print(f"Transactions total rows: {transactions_df.count():,}")
              
          #     # 4. Show Schemas to verify context data types
          #     print("\n--- Transactions Data Schema ---")
          #     transactions_df.printSchema()
          #     articles_df.printSchema() 
          #     customers_df.printSchema()
          #        # 5. Display a sample of the transactions dataframe
          #     print("\n--- Previewing first 5 rows of all csv files ---")
          #     transactions_df.show(5)
          #     customers_df.show(5)
          #     articles_df.show(5)
              
          #     return articles_df, customers_df, transactions_df
          
          # if __name__ == "__main__":
          #     # Start the PySpark session
          #     spark_session = init_spark()
              
          #     # Execute the load function
          # articles, customers, transactions = load_data_with_pyspark(spark_session)

The output for the following codes are 

--- Spark DataFrame Statistics ---
Articles total rows:     105,542
Customers total rows:    1,371,980
Transactions total rows: 31,788,324                                             

--- Transactions Data Schema ---
root
 |-- t_dat: date (nullable = true)
 |-- customer_id: string (nullable = true)
 |-- article_id: integer (nullable = true)
 |-- price: double (nullable = true)
 |-- sales_channel_id: integer (nullable = true)

root
 |-- article_id: integer (nullable = true)
 |-- product_code: integer (nullable = true)
 |-- prod_name: string (nullable = true)
 |-- product_type_no: integer (nullable = true)
 |-- product_type_name: string (nullable = true)
 |-- product_group_name: string (nullable = true)
 |-- graphical_appearance_no: integer (nullable = true)
 |-- graphical_appearance_name: string (nullable = true)
 |-- colour_group_code: integer (nullable = true)
 |-- colour_group_name: string (nullable = true)
 |-- perceived_colour_value_id: integer (nullable = true)
 |-- perceived_colour_value_name: string (nullable = true)
 |-- perceived_colour_master_id: integer (nullable = true)
 |-- perceived_colour_master_name: string (nullable = true)
 |-- department_no: integer (nullable = true)
 |-- department_name: string (nullable = true)
 |-- index_code: string (nullable = true)
 |-- index_name: string (nullable = true)
 |-- index_group_no: integer (nullable = true)
 |-- index_group_name: string (nullable = true)
 |-- section_no: integer (nullable = true)
 |-- section_name: string (nullable = true)
 |-- garment_group_no: integer (nullable = true)
 |-- garment_group_name: string (nullable = true)
 |-- detail_desc: string (nullable = true)

root
 |-- customer_id: string (nullable = true)
 |-- FN: double (nullable = true)
 |-- Active: double (nullable = true)
 |-- club_member_status: string (nullable = true)
 |-- fashion_news_frequency: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- postal_code: string (nullable = true)


--- Previewing first 5 rows of all csv files ---
+----------+--------------------+----------+--------------------+----------------+
|     t_dat|         customer_id|article_id|               price|sales_channel_id|
+----------+--------------------+----------+--------------------+----------------+
|2018-09-20|000058a12d5b43e67...| 663713001|0.050830508474576264|               2|
|2018-09-20|000058a12d5b43e67...| 541518023| 0.03049152542372881|               2|
|2018-09-20|00007d2de826758b6...| 505221004| 0.01523728813559322|               2|
|2018-09-20|00007d2de826758b6...| 685687003|0.016932203389830508|               2|
|2018-09-20|00007d2de826758b6...| 685687004|0.016932203389830508|               2|
+----------+--------------------+----------+--------------------+----------------+
only showing top 5 rows
+--------------------+----+------+------------------+----------------------+---+--------------------+
|         customer_id|  FN|Active|club_member_status|fashion_news_frequency|age|         postal_code|
+--------------------+----+------+------------------+----------------------+---+--------------------+
|00000dbacae5abe5e...|NULL|  NULL|            ACTIVE|                  NONE| 49|52043ee2162cf5aa7...|
|0000423b00ade9141...|NULL|  NULL|            ACTIVE|                  NONE| 25|2973abc54daa8a5f8...|
|000058a12d5b43e67...|NULL|  NULL|            ACTIVE|                  NONE| 24|64f17e6a330a85798...|
|00005ca1c9ed5f514...|NULL|  NULL|            ACTIVE|                  NONE| 54|5d36574f52495e81f...|
|00006413d8573cd20...| 1.0|   1.0|            ACTIVE|             Regularly| 52|25fa5ddee9aac01b3...|
+--------------------+----+------+------------------+----------------------+---+--------------------+
only showing top 5 rows
+----------+------------+-----------------+---------------+-----------------+------------------+-----------------------+-------------------------+-----------------+-----------------+-------------------------+---------------------------+--------------------------+----------------------------+-------------+---------------+----------+----------------+--------------+----------------+----------+--------------------+----------------+------------------+--------------------+
|article_id|product_code|        prod_name|product_type_no|product_type_name|product_group_name|graphical_appearance_no|graphical_appearance_name|colour_group_code|colour_group_name|perceived_colour_value_id|perceived_colour_value_name|perceived_colour_master_id|perceived_colour_master_name|department_no|department_name|index_code|      index_name|index_group_no|index_group_name|section_no|        section_name|garment_group_no|garment_group_name|         detail_desc|
+----------+------------+-----------------+---------------+-----------------+------------------+-----------------------+-------------------------+-----------------+-----------------+-------------------------+---------------------------+--------------------------+----------------------------+-------------+---------------+----------+----------------+--------------+----------------+----------+--------------------+----------------+------------------+--------------------+
| 108775015|      108775|        Strap top|            253|         Vest top|Garment Upper body|                1010016|                    Solid|                9|            Black|                        4|                       Dark|                         5|                       Black|         1676|   Jersey Basic|         A|      Ladieswear|             1|      Ladieswear|        16|Womens Everyday B...|            1002|      Jersey Basic|Jersey top with n...|
| 108775044|      108775|        Strap top|            253|         Vest top|Garment Upper body|                1010016|                    Solid|               10|            White|                        3|                      Light|                         9|                       White|         1676|   Jersey Basic|         A|      Ladieswear|             1|      Ladieswear|        16|Womens Everyday B...|            1002|      Jersey Basic|Jersey top with n...|
| 108775051|      108775|    Strap top (1)|            253|         Vest top|Garment Upper body|                1010017|                   Stripe|               11|        Off White|                        1|                Dusty Light|                         9|                       White|         1676|   Jersey Basic|         A|      Ladieswear|             1|      Ladieswear|        16|Womens Everyday B...|            1002|      Jersey Basic|Jersey top with n...|
| 110065001|      110065|OP T-shirt (Idro)|            306|              Bra|         Underwear|                1010016|                    Solid|                9|            Black|                        4|                       Dark|                         5|                       Black|         1339| Clean Lingerie|         B|Lingeries/Tights|             1|      Ladieswear|        61|     Womens Lingerie|            1017| Under-, Nightwear|Microfibre T-shir...|
| 110065002|      110065|OP T-shirt (Idro)|            306|              Bra|         Underwear|                1010016|                    Solid|               10|            White|                        3|                      Light|                         9|                       White|         1339| Clean Lingerie|         B|Lingeries/Tights|             1|      Ladieswear|        61|     Womens Lingerie|            1017| Under-, Nightwear|Microfibre T-shir...|
+----------+------------+-----------------+---------------+-----------------+------------------+-----------------------+-------------------------+-----------------+-----------------+-------------------------+---------------------------+--------------------------+----------------------------+-------------+---------------+----------+----------------+--------------+----------------+----------+--------------------+----------------+------------------+--------------------+
only showing top 5 rows


spark_session.stop()

print("✅ Spark Session Closed")

