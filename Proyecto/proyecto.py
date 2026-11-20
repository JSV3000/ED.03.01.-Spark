import shutil
from pathlib import Path
from pyspark.sql import SparkSession


# Configuración Inicial y Carga

spark = (SparkSession.builder
    .appName("AnalisisStocks")
    .config("spark.executor.memory", "512m")
    .getOrCreate())

sc = spark.sparkContext

BASE_DIR = Path(__file__).resolve().parent
path_final = (BASE_DIR / "Datos" / "stocks.txt").as_uri()

rdd = sc.textFile(path_final)
print("Primeras 5 líneas crudas:")
print(rdd.take(5))


# Limpieza y Parseo de Datos

def parsear_linea(linea):
    try:
        partes = linea.split(",")
        simbolo = partes[0].strip()
        fecha = partes[1].strip()
        precio = float(partes[2].strip())
        volumen = int(partes[3].strip())
        return (simbolo, fecha, precio, volumen)
    except Exception:
        return None

rdd_parseado = rdd.map(parsear_linea).filter(lambda x: x is not None).cache()
total_registros = rdd_parseado.count()
print(f"\nLíneas válidas: {total_registros:,}")


# Análisis por Símbolo

simbolo_precio = rdd_parseado.map(lambda x: (x[0], x[2]))

precio_max = simbolo_precio.reduceByKey(lambda a, b: max(a, b))
precio_min = simbolo_precio.reduceByKey(lambda a, b: min(a, b))

precio_promedio = simbolo_precio.aggregateByKey(
    (0.0, 0),
    lambda acc, valor: (acc[0] + valor, acc[1] + 1),
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])
).mapValues(lambda x: x[0] / x[1])

print("\nPrecio máximo por símbolo:")
for simbolo, precio in sorted(precio_max.collect()):
    print(f"  {simbolo}: ${precio:.2f}")

print("\nPrecio mínimo por símbolo:")
for simbolo, precio in sorted(precio_min.collect()):
    print(f"  {simbolo}: ${precio:.2f}")

print("\nPrecio promedio por símbolo:")
for simbolo, precio in sorted(precio_promedio.collect()):
    print(f"  {simbolo}: ${precio:.2f}")


# Análisis de Volumen - Top 10 Días

fecha_volumen = rdd_parseado.map(lambda x: (x[1], x[3]))
volumen_por_fecha = fecha_volumen.reduceByKey(lambda a, b: a + b)
top_fechas = volumen_por_fecha.sortBy(lambda x: x[1], ascending=False)

print("\nTop 10 días con mayor volumen:")
for fecha, volumen in top_fechas.take(10):
    print(f"  {fecha}: {volumen:,} acciones")


# Análisis Avanzado - Volatilidad

stats_por_simbolo = simbolo_precio.aggregateByKey(
    (float('-inf'), float('inf')),
    lambda acc, precio: (max(acc[0], precio), min(acc[1], precio)),
    lambda acc1, acc2: (max(acc1[0], acc2[0]), min(acc1[1], acc2[1]))
)

volatilidad = stats_por_simbolo.mapValues(
    lambda x: ((x[0] - x[1]) / x[1]) * 100
)

mas_volatiles = volatilidad.sortBy(lambda x: x[1], ascending=False).take(5)
mas_estables = volatilidad.sortBy(lambda x: x[1], ascending=True).take(5)

print("\nTop 5 símbolos más volátiles:")
for simbolo, vol in mas_volatiles:
    print(f"  {simbolo}: {vol:.2f}%")

print("\nTop 5 símbolos más estables:")
for simbolo, vol in mas_estables:
    print(f"  {simbolo}: {vol:.2f}%")


# Guardar Resultados y Resumen

dir_resultados = BASE_DIR / "RESULTADOS"
dir_resultados.mkdir(parents=True, exist_ok=True)

dir_volumen = dir_resultados / "volumen_por_fecha"
dir_stats = dir_resultados / "stats_por_simbolo"

for d in (dir_volumen, dir_stats):
    if d.exists():
        shutil.rmtree(d)

volumen_por_fecha.saveAsTextFile(dir_volumen.as_uri())
stats_por_simbolo.saveAsTextFile(dir_stats.as_uri())

simbolos_unicos = rdd_parseado.map(lambda x: x[0]).distinct().count()
simbolo_mayor_precio = precio_promedio.sortBy(lambda x: x[1], ascending=False).first()
dia_mayor_volumen = top_fechas.first()
simbolo_mas_volatil = mas_volatiles[0]

print("\n" + "=" * 60)
print("RESUMEN EJECUTIVO - ANÁLISIS DE STOCKS")
print("=" * 60)
print(f"Total de registros procesados: {total_registros:,}")
print(f"Número de símbolos únicos: {simbolos_unicos}")
print(f"Símbolo con mayor precio promedio: {simbolo_mayor_precio[0]} (${simbolo_mayor_precio[1]:.2f})")
print(f"Día con mayor volumen: {dia_mayor_volumen[0]} ({dia_mayor_volumen[1]:,} acciones)")
print(f"Símbolo más volátil: {simbolo_mas_volatil[0]} ({simbolo_mas_volatil[1]:.2f}%)")

spark.stop()
print("\nAnálisis completado. Sesión cerrada.")