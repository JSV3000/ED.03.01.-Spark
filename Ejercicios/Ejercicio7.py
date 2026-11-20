import os
from pyspark.sql import SparkSession

# Corrección 1: Configurar y obtener SparkContext
spark = SparkSession.builder.appName("EjercicioDebugging").getOrCreate()
sc = spark.sparkContext

# Corrección 2: Construir ruta absoluta
path_final = "file://" + os.path.abspath("./datos.csv")
rdd = sc.textFile(path_final)

# Corrección 3: take() en lugar de collect()
muestra = rdd.take(10)
for linea in muestra:
    print(linea)

# Corrección 4: Cerrar sesión y liberar recursos
spark.stop()