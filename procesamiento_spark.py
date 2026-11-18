"""Guía 8 - Implementación equivalente con Apache Spark (PySpark).

Resuelve las MISMAS tres operaciones de la Fase 3 con el motor que la guía
cita como referencia, y compara sus resultados con la versión en Python puro.

Spark se ejecuta en modo local[N]: un único equipo con N hilos ejecutores.
Igual que la simulación con multiprocessing, NO es un clúster real, pero el
código sería el mismo en un clúster (solo cambiaría el parámetro `master`,
p. ej. "spark://host:7077" o "yarn").

Uso:
    pip install pyspark==3.5.3       (requiere Java 8, 11 o 17)
    python procesamiento_spark.py

Salida: resultados/spark_resultados.json
"""

import json
import math
import os
import statistics
import sys
import time

from procesamiento_distribuido import (
    CARPETA_DATOS, CARPETA_RESULTADOS, ejecutar_distribuido_bloques,
    ejecutar_secuencial_completo,
)

NODOS = max(1, int(os.environ.get("LAB_NODOS", "4")))
REPETICIONES = 3
CONJUNTOS = [("ventas_10k.csv", 10_000), ("ventas_100k.csv", 100_000),
             ("ventas_500k.csv", 500_000)]
# Si existe el conjunto grande (python generar_datos.py --grande), también se mide.
if os.path.exists(os.path.join(CARPETA_DATOS, "ventas_2m.csv")):
    CONJUNTOS.append(("ventas_2m.csv", 2_000_000))

# Spark lanza procesos Python auxiliares: deben usar este mismo intérprete.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def crear_sesion():
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .master(f"local[{NODOS}]")                 # N ejecutores locales
        .appName("Guia8-ProcesamientoDistribuido")
        .config("spark.sql.shuffle.partitions", str(NODOS))
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def analizar(spark, ruta):
    """Las tres operaciones de la Fase 3 expresadas como DataFrames de Spark."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                                   StructField, StructType)

    esquema = StructType([
        StructField("id_venta", StringType()),
        StructField("fecha", StringType()),
        StructField("ciudad", StringType()),
        StructField("producto", StringType()),
        StructField("cantidad", IntegerType()),
        StructField("precio_unitario", DoubleType()),
    ])
    ventas = (
        spark.read.csv(ruta, header=True, schema=esquema, encoding="UTF-8")
        .repartition(NODOS)                                   # particionamiento
        .withColumn("ingreso", F.col("cantidad") * F.col("precio_unitario"))
        .withColumn("mes", F.substring("fecha", 1, 7))
        .cache()
    )
    # 1) Ingresos totales por ciudad
    por_ciudad = (ventas.groupBy("ciudad").agg(F.sum("ingreso").alias("ingresos"))
                  .orderBy("ciudad").collect())
    # 2) Top 5 de productos por cantidad
    top5 = (ventas.groupBy("producto").agg(F.sum("cantidad").alias("unidades"))
            .orderBy(F.desc("unidades"), "producto").limit(5).collect())
    # 3) Ingresos totales por mes
    por_mes = (ventas.groupBy("mes").agg(F.sum("ingreso").alias("ingresos"))
               .orderBy("mes").collect())
    registros = ventas.count()
    particiones = ventas.rdd.getNumPartitions()
    ventas.unpersist()
    return {
        "registros": registros,
        "particiones": particiones,
        "ingresos_ciudad": {r["ciudad"]: r["ingresos"] for r in por_ciudad},
        "top_cinco_productos": [
            {"producto": r["producto"], "unidades": int(r["unidades"])} for r in top5],
        "ingresos_mes": {r["mes"]: r["ingresos"] for r in por_mes},
    }


def coincide(spark_res, ref):
    """Compara Spark contra la referencia secuencial (misma tolerancia que verificar())."""
    if spark_res["registros"] != ref["registros"]:
        return False
    if spark_res["top_cinco_productos"] != ref["top_cinco_productos"]:
        return False
    for clave in ("ingresos_ciudad", "ingresos_mes"):
        a, b = spark_res[clave], ref[clave]
        # Misma regla que verificar(): al sumar en otro orden los flotantes
        # difieren en el último decimal (≈1e-14 relativo con 2 millones de filas).
        if set(a) != set(b) or not all(
                math.isclose(a[k], b[k], rel_tol=1e-9, abs_tol=0.01) for k in a):
            return False
    return True


def main():
    try:
        import pyspark
    except ImportError:
        print("PySpark no está instalado. Instálelo con: pip install pyspark==3.5.3")
        sys.exit(1)

    os.makedirs(CARPETA_RESULTADOS, exist_ok=True)
    print(f"=== Apache Spark {pyspark.__version__} en modo local[{NODOS}] ===")
    inicio = time.perf_counter()
    spark = crear_sesion()
    spark.sparkContext.setLogLevel("ERROR")
    t_arranque = time.perf_counter() - inicio
    print(f"Arranque de la sesión Spark (JVM): {t_arranque:.2f} s")

    filas = []
    try:
        for nombre, cantidad in CONJUNTOS:
            ruta = os.path.join(CARPETA_DATOS, nombre)
            referencia, _ = ejecutar_secuencial_completo(ruta)
            analizar(spark, ruta)  # calentamiento (compilación del plan, JIT)
            tiempos, resultado = [], None
            for _ in range(REPETICIONES):
                t0 = time.perf_counter()
                resultado = analizar(spark, ruta)
                tiempos.append(time.perf_counter() - t0)
            t_python = statistics.median(
                ejecutar_secuencial_completo(ruta)[1] for _ in range(REPETICIONES))
            t_bloques = statistics.median(
                ejecutar_distribuido_bloques(ruta, NODOS)[1] for _ in range(REPETICIONES))
            t_spark = statistics.median(tiempos)
            ok = coincide(resultado, referencia)
            del referencia
            filas.append({
                "archivo": nombre,
                "registros": cantidad,
                "particiones_spark": resultado["particiones"],
                "t_python_secuencial_s": round(t_python, 4),
                "t_python_bloques_B_s": round(t_bloques, 4),
                "t_spark_s": round(t_spark, 4),
                "aceleracion_vs_python": round(t_python / t_spark, 3),
                "resultados_iguales": ok,
                "top5": [p["producto"] for p in resultado["top_cinco_productos"]],
            })
            print(f"{cantidad:>9,} reg. | Python secuencial={t_python:6.3f}s | "
                  f"Python B ({NODOS} nodos)={t_bloques:6.3f}s | Spark={t_spark:6.3f}s | S={t_python / t_spark:5.2f}x | idénticos={ok}")
    finally:
        spark.stop()

    with open(os.path.join(CARPETA_RESULTADOS, "spark_resultados.json"), "w",
              encoding="utf-8") as archivo:
        json.dump({
            "version_spark": pyspark.__version__,
            "master": f"local[{NODOS}]",
            "arranque_sesion_s": round(t_arranque, 2),
            "repeticiones": REPETICIONES,
            "nota": "Tiempo de consulta sin incluir el arranque de la JVM; "
                    "un equipo, no un clúster real.",
            "tabla": filas,
        }, archivo, ensure_ascii=False, indent=2)
    print("\nResultados guardados en: resultados/spark_resultados.json")


if __name__ == "__main__":
    main()
