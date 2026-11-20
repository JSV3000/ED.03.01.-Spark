from pyspark.sql import SparkSession
import os

spark = (SparkSession.builder
    .appName("MiPrimerAnalisis")
    .config("spark.executor.memory", "1g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate())

sc = spark.sparkContext

rdd = sc.textFile("file://" + os.path.abspath("./DATOS/datos.csv"))

def parsear_persona(linea):
    cols = linea.split(",")
    return (cols[0].strip(), cols[1].strip(), cols[2].strip())

personas_rdd = rdd.map(parsear_persona)