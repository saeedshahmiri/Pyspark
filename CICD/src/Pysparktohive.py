from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, upper, concat, length, current_date, sum

# Create spark session with hive enabled
spark = SparkSession.builder \
        .appName("BankMarketing") \
        .config("spark.sql.warehouse.dir", "/warehouse/tablespace/external/hive") \
        .enableHiveSupport() \
        .getOrCreate()

## Initialize SparkSession with Hive support
# Replace with your Hive warehouse directory


## 1- Establish the connection to PostgresSQL and read data from the postgres Database -testdb
# PostgresSQL connection properties
postgres_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
postgres_properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver"
}


try:
    postgres_table_name = "bank-full"

    # read data from postgres table into dataframe :
    df_postgres = spark.read.jdbc(url=postgres_url, table=postgres_table_name, properties=postgres_properties)
    df_postgres.printSchema()
    df_postgres.show(3)

    #-+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+-
    #-+-+--+-+--+-+--+-+--+-+-Transformations-+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+-
    #-+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+--+-+-

    # change job column into upper case
    # df_upper = df_postgres.withColumn("job_upper", upper(df_postgres['job']))
    # df_upper.show(5)

    ## 2. load df_postgres to hive table
    # Create database
    spark.sql("CREATE DATABASE IF NOT EXISTS tekle")

    # Hive database and table names
    hive_database_name = "sdata124"
    hive_table_name = "bank-full"

   # Create Hive Internal table over project1db
    df_postgres.write.mode('overwrite').saveAsTable("{}.{}".format(hive_database_name, hive_table_name))

    print("Data saved successfully to Hive table: ", hive_table_name)

    # Read Hive table
    df = spark.read.table("{}.{}".format(hive_database_name, hive_table_name))
    df.show()

except Exception as e:
    print("Error reading data from PostgreSQL or saving to Hive:", e)
finally:
    spark.stop()

    #spark-submit --jars postgresql-42.6.0.jar myspark_hive.py