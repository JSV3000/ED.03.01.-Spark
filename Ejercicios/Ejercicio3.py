from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("MiPrimerAnalisis")
    .config("spark.executor.memory", "1g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate())

sc = spark.sparkContext

numeros = sc.parallelize(range(1, 51))

# 1. Multiplicar cada número por 3
multiplicados = numeros.map(lambda x: x * 3)

# 2. Filtrar mayores a 50
mayores_50 = multiplicados.filter(lambda x: x > 50)

# 3 y 4. Contar y mostrar primeros 10 resultados
print(f"Total de números mayores a 50: {mayores_50.count()}")
print(f"Primeros 10: {mayores_50.take(10)}")