# When runing on Jenkin, take care of permistions with "chmod -R 777" command 

from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, upper

# Create spark session with hive enabled
spark = SparkSession.builder \
        .appName("bank-full") \
        .config("spark.sql.warehouse.dir", "/warehouse/tablespace/external/hive") \
        .enableHiveSupport() \
        .getOrCreate()

# 1- Establish the connection to PostgresSQL and hive:

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

   
    

    # change job column into upper case
    # df_postgres = df_postgres.withColumn("job_upper", upper(df_postgres['job']))
    # df_upper.show(5)

    ## 2. load df_postgres to hive table
    # Create database
    spark.sql("CREATE DATABASE IF NOT EXISTS tekle")

    # Hive database and table names
    hive_database_name = "sdata124"
    hive_table_name = "bank-full"

    # read and show the existing_data in hive table
    existing_hive_data = spark.read.table("{}.{}".format(hive_database_name, hive_table_name))
    existing_hive_data.show()

    # 4. Determine the incremental data
    '''
    "left_anti" specifies the type of join to perform. 
    A left_anti join returns only the rows from the left DataFrame (df_upper in this case) 
    that do not have corresponding matches in the right DataFrame (existing_hive_data). 
    Essentially, it finds rows in df_upper that are not present in existing_hive_data based 
    on id
    '''
    incremental_data_df = df_postgres.join(existing_hive_data.select("id"), df_postgres["id"] == existing_hive_data["id"], "left_anti")
    print('------------------Incremental data-----------------------')
    incremental_data_df.show()

    # counting the number of the new records added to postgres tables
    new_records = incremental_data_df.count()
    print('------------------COUNTING INCREMENT RECORDS ------------')
    print('new records added count', new_records)

    # 5.  Adding the incremental_data DataFrame to the existing hive table
    # Check if there are extra rows in PostgresSQL. if exist => # write & append to the Hive table
    if incremental_data_df.count() > 0:
        # Append new rows to Hive table
        incremental_data_df.write.mode("append").insertInto("{}.{}".format(hive_database_name, hive_table_name))
        #incremental_data_df.write.mode('overwrite').saveAsTable("{}.{}".format(hive_database_name, hive_table_name))
        print("Appended {} new records to Hive table.".format(incremental_data_df.count()))
    else:
        print("No new records been inserted in PostgresSQL table.")

    # Read again from hive to see the updated data
    updated_hive_data = spark.read.table("{}.{}".format(hive_database_name, hive_table_name))
    df_ordered = updated_hive_data.orderBy(updated_hive_data["id"].desc())
    df_ordered.show()
except Exception as ex:
    print("Error reading data from PostgreSQL or saving to Hive:", ex)

finally:
    spark.stop()


# spark-submit --jars postgresql-42.6.0.jar pyspark_incrementalLoad.py

