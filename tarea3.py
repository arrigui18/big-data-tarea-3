# Importamos las librerías necesarias
from pyspark.sql import SparkSession, functions as F

# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('Tarea3').getOrCreate()

# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea3/rows.csv'

# Lee el archivo .csv
df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(file_path)

# Imprimimos el esquema
df.printSchema()

# Muestra las primeras filas del DataFrame
df.show()

# Estadísticas básicas
df.summary().show()

# ----------- Limpieza de datos -----------

# Elimina filas con valores nulos
df_clean = df.na.drop()

# Elimina filas duplicadas
df_clean = df_clean.dropDuplicates()

# Imprimir el esquema y las primeras filas del DataFrame limpio
df_clean.printSchema()
df_clean.show()

# ----------- Transformaciones de datos -----------

# convertir a tipo Date
df_clean = df_clean.withColumn('fecha reporte', F.to_date('fecha reporte', 'yyyy-MM-dd'))

# Crear una nueva columna que calcule la diferencia en días desde la fecha actual
df_clean = df_clean.withColumn('dias_desde_fecha', F.datediff(F.current_date(), df_clean['fecha reporte']))

# Verificar las transformaciones
df_clean.show()

# ----------- Análisis Exploratorio de Datos (EDA) -----------

# Contar la frecuencia de una columna
df_clean.groupBy('Tipo de contagio').count().show()

# Calcular estadísticas descriptivas de una columna numérica
df_clean.select('ID de caso').describe().show()

# Calcular la media de una columna numérica
media_valor = df_clean.agg(F.mean('ID de caso')).collect()[0][0]
print(f'Media del valor: {media_valor}')

# ----------- Almacenar resultados procesados -----------

# Almacenar el DataFrame limpio en HDFS
output_path = 'hdfs://localhost:9000/Tarea3/cleaned_rows.csv'
df_clean.write.format('csv').option('header', 'true').save(output_path)

# Almacenar los resultados de análisis (frecuencias) en HDFS
frecuencia_df = df_clean.groupBy('Tipo de contagio').count()
output_freq_path = 'hdfs://localhost:9000/Tarea3/frecuencia.csv'
frecuencia_df.write.format('csv').option('header', 'true').save(output_freq_path)

# Detener la sesión de Spark
spark.stop()