"""Experimento complementario: escenario CPU-intensivo.

La agregación simple de la guía es tan rápida que el costo de arrancar
procesos y serializar datos domina el tiempo total. Este experimento añade
un cálculo derivado más costoso por registro (un "índice de riesgo" usando
operaciones matemáticas) para observar cuándo el paralelismo sí produce
aceleración.

Uso:
    python experimento_intensivo.py
"""

import json
import math
import os
import time
import multiprocessing as mp

import statistics

from procesamiento_distribuido import (
    BASE, CARPETA_RESULTADOS, NODOS, leer_csv, particionar,
)

REPETICIONES = 3  # se reporta la mediana

ITERACIONES = 120
ARCHIVO = "ventas_100k.csv"


def procesar_particion_intensiva(filas):
    """Agregados del laboratorio más un cálculo derivado CPU-bound."""
    inicio = time.perf_counter()
    indice_total = 0.0
    ingresos_ciudad = {}
    unidades_producto = {}
    ingresos_mes = {}
    total_ingresos = 0.0
    total_unidades = 0

    for _id, fecha, ciudad, producto, cantidad, precio in filas:
        ingreso = cantidad * precio
        ingresos_ciudad[ciudad] = ingresos_ciudad.get(ciudad, 0.0) + ingreso
        unidades_producto[producto] = unidades_producto.get(producto, 0) + cantidad
        ingresos_mes[fecha[:7]] = ingresos_mes.get(fecha[:7], 0.0) + ingreso
        total_ingresos += ingreso
        total_unidades += cantidad

        semilla = (cantidad * 31 + int(precio)) / 1000.0
        acumulador = semilla
        for k in range(1, ITERACIONES + 1):
            acumulador += math.sin(k * semilla) * math.cos(k / semilla + 1)
        indice_total += abs(acumulador)

    return {
        "registros": len(filas),
        "ingresos_ciudad": ingresos_ciudad,
        "unidades_producto": unidades_producto,
        "ingresos_mes": ingresos_mes,
        "ingresos_totales": total_ingresos,
        "unidades_totales": total_unidades,
        "indice_total": indice_total,
        "tiempo_calculo": time.perf_counter() - inicio,
    }


def una_medicion(filas):
    """Devuelve (t_sec, t_dist, parciales, seq, t_part, t_pool, t_cons)."""
    inicio = time.perf_counter()
    seq = procesar_particion_intensiva(filas)
    t_sec = time.perf_counter() - inicio

    inicio = time.perf_counter()
    partes = particionar(filas, NODOS)
    t_part = time.perf_counter() - inicio

    inicio = time.perf_counter()
    with mp.Pool(processes=NODOS) as pool:
        parciales = pool.map(procesar_particion_intensiva, partes)
    t_pool = time.perf_counter() - inicio

    inicio = time.perf_counter()
    sum(p["indice_total"] for p in parciales)
    t_cons = time.perf_counter() - inicio
    return t_sec, t_part + t_pool + t_cons, parciales, seq, t_part, t_pool, t_cons


def main():
    mp.freeze_support()
    os.makedirs(CARPETA_RESULTADOS, exist_ok=True)
    filas = leer_csv(os.path.join(BASE, "datos", ARCHIVO))

    mediciones = [una_medicion(filas) for _ in range(REPETICIONES)]
    t_sec = statistics.median(m[0] for m in mediciones)
    t_dist = statistics.median(m[1] for m in mediciones)
    _, _, parciales, seq, t_part, t_pool, t_cons = mediciones[-1]
    indice_dist = sum(p["indice_total"] for p in parciales)
    tz = abs(seq["indice_total"] - indice_dist) / max(seq["indice_total"], 1e-9)

    resultado = {
        "registros": len(filas),
        "iteraciones_por_registro": ITERACIONES,
        "nodos": NODOS,
        "repeticiones": REPETICIONES,
        "tiempo_secuencial_s": round(t_sec, 4),
        "tiempo_distribuido_s": round(t_dist, 4),
        "tiempo_calculo_nodos_s": round(sum(p["tiempo_calculo"] for p in parciales), 4),
        "costo_coordinacion_s": round(
            t_part + t_cons + (t_pool - max(p["tiempo_calculo"] for p in parciales)), 4
        ),
        "aceleracion": round(t_sec / t_dist, 3),
        "eficiencia": round((t_sec / t_dist) / NODOS, 3),
        "diferencia_relativa_indice": round(tz, 12),
        # La suma de flotantes en distinto orden puede variar en ~1e-15.
        "resultados_iguales": tz < 1e-9,
    }

    with open(
        os.path.join(CARPETA_RESULTADOS, "escenario_intensivo.json"), "w",
        encoding="utf-8",
    ) as archivo:
        json.dump(resultado, archivo, ensure_ascii=False, indent=2)

    print(json.dumps(resultado, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
