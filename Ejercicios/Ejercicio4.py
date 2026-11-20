from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("MiPrimerAnalisis")
    .config("spark.executor.memory", "1g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate())

sc = spark.sparkContext

ventas = [
    ("Laptop", 1200),
    ("Mouse", 25),
    ("Laptop", 1100),
    ("Teclado", 75),
    ("Mouse", 30),
    ("Laptop", 1300)
]

rdd_ventas = sc.parallelize(ventas)

# 2. Total de ventas por producto
total_por_producto = rdd_ventas.reduceByKey(lambda a, b: a + b)

# 3. Cantidad de ventas por producto
conteo_por_producto = rdd_ventas.mapValues(lambda v: 1).reduceByKey(lambda a, b: a + b)

# 4. Producto con mayor total de ventas
mayor_producto = total_por_producto.sortBy(lambda x: x[1], ascending=False).first()

print("Total por producto:", total_por_producto.collect())
print("Número de ventas:", conteo_por_producto.collect())
print(f"Producto con mayor total: {mayor_producto[0]} ({mayor_producto[1]})")