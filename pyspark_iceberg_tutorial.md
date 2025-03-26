
# 🧠 PySpark + Iceberg – Guía Rápida

## 🔧 1. Crear sesión de Spark (en scripts Python)
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MiScript")     .getOrCreate()
```
⚠️ No es necesario añadir `.config(...)` si ya tienes el `spark-defaults.conf` configurado.

---

## 🧊 2. Leer tablas Iceberg
```python
df = spark.read.table("demo.db.mi_tabla")
spark.sql("SELECT * FROM demo.db.mi_tabla WHERE campo = 'valor'").show()
```

---

## 🧊 3. Escribir DataFrames a Iceberg
- Crear nueva tabla:
```python
df.writeTo("demo.db.mi_tabla").create()
```
- Reemplazar:
```python
df.writeTo("demo.db.mi_tabla").overwritePartitions()
```
- Añadir:
```python
df.writeTo("demo.db.mi_tabla").append()
```

---

## 🔍 4. Explorar tablas y bases de datos
```python
spark.sql("SHOW NAMESPACES IN demo").show()
spark.sql("SHOW TABLES IN demo.db").show()
spark.sql("DESCRIBE TABLE demo.db.mi_tabla").show()
```

---

## 🔗 5. Joins, filtros y selecciones
```python
df1.join(df2, "id_usuario")
df.filter(col("evento") == "no_entrega")
df.select("id", "nombre", "nota")
```

Alternativa con SQL:
```sql
SELECT u.id, u.nombre, COUNT(*) as fallos
FROM usuarios u
JOIN logs l ON u.id = l.usuario_id
WHERE l.evento = 'no_entrega'
GROUP BY u.id, u.nombre
```

---

## 🗃️ 6. Crear bases de datos y tablas (Iceberg SQL)
```sql
CREATE DATABASE IF NOT EXISTS demo.moodle;

CREATE TABLE demo.moodle.estudiantes (
  id_usuario BIGINT,
  nombre STRING,
  email STRING
) USING iceberg;
```

---

## ✍️ 7. Registrar vistas temporales para usar SQL
```python
df_logs.createOrReplaceTempView("logs")
df_users.createOrReplaceTempView("usuarios")
spark.sql("SELECT * FROM logs JOIN usuarios ON ...")
```

---

## 💾 8. Guardar resultados
```python
df.writeTo("demo.moodle.resultados_finales").createOrReplace()
df.write.mode("overwrite").parquet("salidas/final.parquet")
```

---

## 🧪 9. Acciones para ejecutar el plan
```python
df.show()
df.collect()
df.write...
df.count()
```

---

## 🧰 10. Funciones útiles
```python
from pyspark.sql.functions import col, when, count, avg, sum, desc, row_number
from pyspark.sql.window import Window
```

- `when(cond, val).otherwise(...)`: condicional
- `row_number().over(Window.partitionBy(...).orderBy(...))`: ranking
- `groupBy(...).agg(...)`: agregación
