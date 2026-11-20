from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("MiPrimerAnalisis")
    .config("spark.executor.memory", "1g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate())

sc = spark.sparkContext

from pathlib import Path

ruta_archivo = Path(__file__).resolve().parent / "Datos" / "persons.csv"
path_final = ruta_archivo.as_uri()
rdd_csv = sc.textFile(path_final)

# Eliminar encabezado
header = rdd_csv.first()
rdd_sin_header = rdd_csv.filter(lambda linea: linea != header)

# Extraer (nombre, edad) haciendo un solo split por línea
def extraer_nombre_edad(linea):
    cols = linea.split(",")
    return (cols[0].strip(), int(cols[1].strip()))

personas = rdd_sin_header.map(extraer_nombre_edad)

# Filtrar mayores de 30 años
mayores_30 = personas.filter(lambda x: x[1] > 30)

resultados = mayores_30.collect()
print("Personas mayores de 30:", [f"{n} ({e})" for n, e in resultados])
print(f"Total: {mayores_30.count()} personas")