from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("MiPrimerAnalisis")
    .config("spark.executor.memory", "1g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate())

sc = spark.sparkContext
numeros = list(range(1, 21))

rdd_numeros = sc.parallelize(numeros)

print(f"Total de elementos: {rdd_numeros.count()}")
print(f"Primeros 5: {rdd_numeros.take(5)}")
print(f"Suma: {rdd_numeros.sum()}")
print(f"Máximo: {rdd_numeros.max()}, Mínimo: {rdd_numeros.min()}")