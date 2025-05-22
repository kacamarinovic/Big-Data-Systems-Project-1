import sys
import re
import os
import math
from math import radians, sin, cos, sqrt, atan2
import pyspark.sql.functions as sparkFun
import time

from pyspark.sql import SparkSession

def get_arguments():
    # argumenti promenljive okruzenja - pokretanje uz pomoc Docker-a
    spark_args = os.getenv('SPARK_APPLICATION_ARGS')
    if spark_args:
        return spark_args.split()
    
    # Argumenti komandne linije - lokalno pokretanje
    if len(sys.argv) > 1:
        return sys.argv[1:]
    

def filter_vehicles(df, lat, lon, radius):
    #1 stepen geokoordinate je priblizno 111319 m
    lat_to_meters = 111319
    lon_to_meters = 111319 * math.cos(math.radians(lat))

    #Dodajemo kolonu sa izračunatom udaljenošću od zadate tačke
    df_with_distance = df.withColumn(
        'Distance',
        sparkFun.sqrt(
            ((sparkFun.col('vehicle_x') - sparkFun.lit(lon)) * lon_to_meters) ** 2 +
            ((sparkFun.col('vehicle_y') - sparkFun.lit(lat)) * lat_to_meters) ** 2
        )
    )

    # Filtriranje na osnovu udaljenosti
    filtered_df = df_with_distance.filter(sparkFun.col('Distance') < radius)
    return filtered_df

    

if __name__ == "__main__":

    print("hello")

    args = get_arguments()

    if len(args) != 6 and len(args) != 7:
       print("Usage: file.py <coordinateX> <coordinateY> <radius> <timestamp1> <timestamp2> <fuel/pollution> [<vehicle_type>]")
       exit(-1)



    appName = "Big Mobility and IoT Data Analytics"
    print(appName)

    # script_directory = os.path.dirname(os.path.abspath(__file__))
    # input_file_fcd = os.path.join(script_directory, "input_folder", "fcd1.csv")
    # input_file_emissions = os.path.join(script_directory, "input_folder", "emission1.csv")


    input_file_fcd = "hdfs://namenode:9000/input_folder/fcd1.csv"
    input_file_emissions = "hdfs://namenode:9000/input_folder/emission1.csv"


    lat = float(args[0])
    lon = float(args[1])
    radius = float(args[2])
    ts1 = args[3]
    ts2 = args[4]
    parameter = args[5]

    if len(args) == 7:
        veh_type = args[6]

    #spark = SparkSession.builder.appName(appName).getOrCreate()
    #spark = SparkSession.builder.appName(appName).master("local[*]").getOrCreate()
    
    spark = SparkSession.builder.appName(appName).master("spark://spark-master:7077").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")      

    #zadatak 1: vozila koja su prolazila u blizini značajnih mesta u odredjenom periodu
   
    df = spark.read.option("delimiter", ";").option("inferSchema", True).option("header", True).csv(input_file_fcd)
    
    result = df.filter('timestep_time > '+ts1+' and timestep_time < '+ts2) 
    
    if len(args) == 7:
        result = result.filter("vehicle_type == '{}'".format(veh_type))

    result = filter_vehicles(result, lat, lon, radius)

    print(f"\nInformacije o vozilima koja su se u datom vremenskom periodu nalazila u blizini zadate lokacije:\n")
    result.show()

    result_distinct = result.select("vehicle_id", "vehicle_type").distinct()

    print(f"\nBroj jedinstvenih vozila: {result_distinct.count()}\n")  
    result_distinct.show()
    
    #zadatak 2: parametri zagadjenja ili potrosnje goriva po ulicama u odredjenom vremenskom periodu

    df2 = spark.read.option("delimiter", ";").option("inferSchema", True).option("header", True).csv(input_file_emissions)    
    result2 = df2.filter('timestep_time > '+ts1+' and timestep_time < '+ts2) 

    if(parameter == "fuel"):
        print(f"\n\nPotrosnja goriva po ulicama:\n")
        fuel_df = result2.groupBy("vehicle_lane").agg(
            sparkFun.min("vehicle_fuel").alias("min_fuel"),
            sparkFun.max("vehicle_fuel").alias("max_fuel"),
            sparkFun.avg("vehicle_fuel").alias("avg_fuel"),
            sparkFun.stddev("vehicle_fuel").alias("stddev_fuel"),
            sparkFun.min("vehicle_electricity").alias("min_electricity"),
            sparkFun.max("vehicle_electricity").alias("max_electricity"),
            sparkFun.avg("vehicle_electricity").alias("avg_electricity"),
            sparkFun.stddev("vehicle_electricity").alias("stddev_electricity")
        )
        fuel_df.show()

    if(parameter == "pollution"):
        print(f"Parametri zagadjenja po ulicama:")
        parameters = ["vehicle_CO","vehicle_CO2","vehicle_HC","vehicle_NOx","vehicle_PMx","vehicle_noise"]
        for parameter in parameters:
            print(f"\n\nParametri zagadjenja {parameter} :\n")
            pollution_df = result2.groupBy("vehicle_lane").agg(
                sparkFun.min(parameter).alias("min_pollution"),
                sparkFun.max(parameter).alias("max_pollution"),
                sparkFun.avg(parameter).alias("avg_pollution"),
                sparkFun.stddev(parameter).alias("stddev_pollution")
            )
            pollution_df.show()
    

    time.sleep(120)
    spark.stop()
