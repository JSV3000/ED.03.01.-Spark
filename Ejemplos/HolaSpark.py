from pyspark.sql import SparkSession

# 1. Crear SparkSession
spark = SparkSession.builder.appName("HolaSpark").getOrCreate()

# 2. Obtener el SparkContext (para funciones de bajo nivel)
sc = spark.sparkContext

# 3. Crear un RDD (Resilient Distributed Dataset)
# parallelize() distribuye los datos en particiones
numeros = sc.parallelize(range(101))

# 4. Realizar una acción: sumar todos los números
total = numeros.sum()
print(f"La suma de 0 a 100 es: {total}")
# Salida: La suma de 0 a 100 es: 5050

# 5. Verificar cuántas particiones se crearon
print(f"Número de particiones: {numeros.getNumPartitions()}")