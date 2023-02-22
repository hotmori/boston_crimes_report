import sys, os
from pyspark.sql import SparkSession, Window
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


    crimes_path = args[0]
    offense_codes_path = args[1]
    result_path = args[2]

    print(f'{crimes_path=}')
    print(f'{offense_codes_path=}')
    print(f'{result_path=}')



    df_crime = spark.read.option("header","true").option("inferSchema", "true").csv(crimes_path)
    df_offense = spark.read.option("header","true").option("inferSchema", "true").csv(offense_codes_path)


    #Remove duplicates
    df_offense_clean = df_offense.withColumn("CRIME_TYPE",trim(split(col("NAME"), ' - ')[0])) \
                            .dropDuplicates(["CODE"])


    #df_offense_clean.filter(col("CODE") == 3502).show()

    df_crime_clean_1 = df_crime \
                        .join(df_offense_clean, col("OFFENSE_CODE") == col("CODE"), "left") \
                        .select("INCIDENT_NUMBER", "CRIME_TYPE", "DISTRICT", "YEAR","MONTH", "LAT", "LONG") \
                        .na \
                        .fill("NA")

                                      
    df_crime_clean_2 = df_crime_clean_1.dropDuplicates(["INCIDENT_NUMBER", "CRIME_TYPE"])

    # Aggregates
    q1_distr = df_crime_clean_2 \
              .groupBy("DISTRICT", "YEAR", "MONTH") \
              .agg(countDistinct("INCIDENT_NUMBER").alias("COUNT")) \
              .groupBy("DISTRICT") \
              .agg(sum(col("COUNT")).alias("CRIMES_TOTAL"), mean(col("COUNT")).alias("CRIMES_MONTHLY") ) \
              .orderBy("DISTRICT")

    #q1_distr.show()


    windowFrequentSpec = Window.partitionBy("DISTRICT").orderBy(desc("COUNT"))
    q2_freq_crimes = df_crime_clean_2 \
                .groupBy("DISTRICT", "CRIME_TYPE") \
                .count().alias("COUNT") \
                .withColumn("ROW_NUMBER", row_number().over(windowFrequentSpec)) \
                .filter(col("ROW_NUMBER") <= 3) \
                .drop("ROW_NUMBER") \
                .groupBy("DISTRICT") \
                .agg(collect_list("CRIME_TYPE").alias("CRIME_TYPE_ARR")) \
                .withColumn("FREQUENT_CRIME_TYPES", concat_ws(",",col("CRIME_TYPE_ARR"))) \
                .drop("CRIME_TYPE_ARR")

    q3_coord = df_crime_clean_2 \
          .filter(col("LAT").isNotNull() & col("LONG").isNotNull()) \
          .groupBy("DISTRICT") \
          .agg(avg("LAT").alias("lat"),avg("LONG").alias("lng") )

    # Consolidation
    df_result = q1_distr \
            .join(q2_freq_crimes, "DISTRICT", "left") \
            .join(q3_coord, "DISTRICT", "left") \
            .orderBy("DISTRICT")

    df_result.show(99, truncate = False)
    df_result.write.mode("overwrite").parquet(result_path)

    spark.stop()                                                                                                                                               
