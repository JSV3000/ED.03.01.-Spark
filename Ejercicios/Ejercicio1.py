from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("MiPrimerAnalisis")
    .config("spark.executor.memory", "1g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate())

# Verificar configuración e información de la sesión
print(spark)
print("AppName:", spark.sparkContext.appName)
print("Executor Memory:", spark.conf.get("spark.executor.memory"))
print("Shuffle Partitions:", spark.conf.get("spark.sql.shuffle.partitions"))