import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, date
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Boston Crimes PySpark") \
        .getOrCreate()

    print("spark.version ==", spark.version)


    #sc = spark.sparkContext




    print('------------------------------------------------------------------------------------------')
    print('------------------------------------------------------------------------------------ START')
    print('------------------------------------------------------------------------------------------')


    args = sys.argv[1:]
    print('args: ' + str(args))

    if len(args) != 3:
        print('Usage: spark-submit <full path crimes.csv> <full path offense_codes.csv> <output dir>')
        exit(1)


    prefix = 'hdfs://'
    crimes_path = prefix + args[0]
    offense_codes_path = prefix + args[1]
    result_path = args[2]

    print(f'{crimes_path=}')
    print(f'{offense_codes_path=}')
    print(f'{result_path=}')



    df_crime = spark.read.option("header","true").option("inferSchema", "true").csv(crimes_path)
    df_crime.show()
    spark.stop()
