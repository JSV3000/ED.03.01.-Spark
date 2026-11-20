# ED.03.01 - Apache Spark Standalone

**Autor:** Josue Sifuentes Valdez  
**Universidad Politécnica de Querétaro — IDIA 224**

---

## Ejercicio 6: Optimización - Evitar Múltiples `split()`

**¿Cuál es el problema de eficiencia en el código original?**  
El código original ejecuta `split(",")` tres veces distintas sobre el mismo RDD (una para extraer nombres, otra para edades y otra para ciudades). Esto obliga a Spark a recorrer los datos tres veces y a dividir repetidamente cada cadena en memoria.

**¿Cómo queda el código optimizado haciendo `split()` una sola vez para extraer nombre, edad y ciudad en una sola pasada?**
```python
def parsear_persona(linea):
    cols = linea.split(",")
    return (cols[0].strip(), cols[1].strip(), cols[2].strip())

personas_rdd = rdd.map(parsear_persona)
```

**¿Por qué esta solución es más eficiente?**  
Porque cada línea se lee y se divide con `split(",")` una única vez, devolviendo los tres campos juntos en una tupla durante una sola pasada. Esto reduce el uso de CPU en el parseo, evita crear listas temporales repetidas en memoria y elimina lecturas redundantes del archivo.

---

## Ejercicio 7: Debugging - Encuentra y Corrige Errores

**¿Cuáles son los 4 errores del código y por qué cada uno es un error?**  
1. **Falta obtener el `SparkContext`:** Se utiliza `sc.textFile(...)` sin haber definido `sc = spark.sparkContext`, lo que genera un `NameError`.
2. **Ruta incorrecta para archivo local:** Al usar `"./datos.csv"` sin ruta absoluta ni el prefijo `file:///`, Spark intenta buscar el archivo en HDFS o falla al resolver el sistema de archivos local (`FileNotFoundException` / `IllegalArgumentException`).
3. **Uso peligroso de `collect()` en datasets grandes:** `collect()` trae todos los datos al Driver y puede provocar un `OutOfMemoryError` (`Py4JJavaError`). Para inspeccionar datos debe usarse `take(n)` o `foreach()`.
4. **No se cierra la sesión de Spark:** Omitir `spark.stop()` deja recursos de CPU y RAM ocupados y mantiene bloqueado el puerto `4040` de la Spark UI.

**¿Cómo queda el código corregido?**
```python
from pathlib import Path
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("EjercicioDebugging").getOrCreate()
sc = spark.sparkContext

path_final = (Path(__file__).resolve().parent / "Datos" / "persons.csv").as_uri()
rdd = sc.textFile(path_final)

for linea in rdd.take(10):
    print(linea)

spark.stop()
```

---

## Preguntas de Reflexión

### 1. Eficiencia
**¿Por qué es importante hacer `split()` una sola vez por línea? ¿Cuánto tiempo ahorrarías con 1 millón de líneas?**  
Hacer `split()` una sola vez evita procesar la misma cadena repetidamente y reduce la creación de listas temporales en memoria. Con 1 millón de líneas y 3 columnas, hacer `split()` por separado ejecuta 3 millones de divisiones y recorre el RDD 3 veces; hacerlo en una sola función ejecuta únicamente 1 millón de divisiones en una sola pasada, ahorrando aproximadamente dos terceras partes del tiempo de CPU dedicado al parseo.

### 2. Particionamiento
**¿Cuántas particiones tiene tu RDD? ¿Cómo afecta esto al rendimiento? ¿Qué pasaría si tuvieras solo 1 partición?**  
El número de particiones se consulta con `rdd.getNumPartitions()` (en modo local con `textFile()` suele ser mínimo 2 para archivos pequeños, o igual al número de núcleos disponibles al usar `parallelize()`). Spark asigna una tarea en paralelo por cada partición, por lo que tener múltiples particiones aprovecha todos los núcleos del equipo. Si existiera solo 1 partición, se perdería por completo el paralelismo: un único núcleo procesaría todo el archivo de forma secuencial mientras el resto permanece inactivo.

### 3. Lazy Evaluation
**¿En qué momento se ejecutaron realmente las transformaciones? ¿Qué ventajas tiene este modelo?**  
Las transformaciones (`map`, `filter`, `reduceByKey`, `aggregateByKey`, `sortBy`) no se ejecutaron al definirse; solo construyeron el plan de ejecución (DAG). Se ejecutaron realmente hasta que se invocó una acción (`take()`, `count()`, `collect()`, `first()`, `saveAsTextFile()`). Su ventaja es que Spark optimiza el flujo completo antes de ejecutarlo, combina operaciones en menos etapas, evita cálculos innecesarios (como al pedir solo `take(5)`) y permite recuperar particiones perdidas ante fallos.

### 4. Memoria
**¿Qué pasaría si intentaras hacer `collect()` en un RDD de 10GB? ¿Cómo evitarías este problema?**  
`collect()` intentaría transferir los 10 GB completos desde los executors hacia la memoria RAM del Driver, provocando un `OutOfMemoryError` (`Py4JJavaError`) al superar su capacidad de memoria. Para evitarlo, se debe usar `take(n)` para inspeccionar muestras pequeñas, hacer filtrados y agregaciones distribuidas antes de recolectar resultados, o guardar la salida directamente en disco/HDFS con `saveAsTextFile()`.

### 5. Optimización
**Si tuvieras que procesar 100x más datos, ¿qué cambiarías en tu código? ¿Usarías más particiones? ¿Cachearías algún RDD?**  
* Migraría de RDDs a **DataFrames** (`spark.read.csv(...)`) para aprovechar el optimizador Catalyst y el manejo eficiente de memoria de Spark SQL.
* **Sí usaría más particiones**, ajustando `repartition()` y `spark.sql.shuffle.partitions` para distribuir la carga de forma equilibrada entre los executors.
* **Sí cachearía el RDD/DataFrame limpio (`rdd_parseado`)** con `.cache()` o `.persist(StorageLevel.MEMORY_AND_DISK)` porque se reutiliza en múltiples cálculos (precios, volumen y volatilidad), evitando releer y volver a parsear el archivo desde disco.
* Aumentaría `spark.executor.memory` y `spark.executor.cores`, evitaría llamadas innecesarias a `collect()` y revisaría la pestaña *Stages* de la Spark UI para detectar *data skew*.

### 6. HDFS vs Local
**¿Cuáles son las ventajas de usar HDFS en lugar de archivos locales para este análisis?**  
HDFS divide los archivos grandes en bloques distribuidos entre múltiples nodos, permitiendo almacenar y procesar volúmenes de datos que no cabrían en el disco de una sola máquina. Además, ofrece tolerancia a fallos mediante réplicas automáticas (si un nodo falla, Spark lee desde otra réplica sin interrumpir el trabajo) y localidad de datos, ejecutando las tareas cerca de donde residen los bloques para reducir el tráfico de red.
