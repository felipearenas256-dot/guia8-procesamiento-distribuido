"""Guía 8 - Fases 3, 4 y 5: implementación, pruebas y análisis.

Simula un procesamiento distribuido mediante PARTICIONES ejecutadas por
procesos independientes (multiprocessing). NO es un clúster real: todos los
"nodos" están en el mismo equipo y comparten CPU, memoria y disco.

Se comparan DOS estrategias de distribución:

  A) "Envío de registros" (round-robin en memoria)
     El coordinador lee TODO el CSV, reparte las filas (i mod N) y las envía
     serializadas a cada nodo. Es el diseño ingenuo: mucho tráfico.

  B) "Localidad de datos" (bloques del archivo, al estilo HDFS/Spark)
     El coordinador solo calcula límites de bytes alineados a fin de línea.
     Cada nodo abre el archivo, lee y analiza SU bloque, y devuelve únicamente
     agregados pequeños. Es el principio "mover el cómputo hacia los datos".

Cada medición se repite REPETICIONES veces y se reporta la MEDIANA (y la
desviación estándar) para reducir el ruido del sistema operativo.

Uso:
    python procesamiento_distribuido.py

Variables de entorno (las usa la interfaz gráfica):
    LAB_NODOS        número de nodos simulados (por defecto 4)
    LAB_DATASET      todos | 10k | 100k | 500k   (por defecto todos)
    LAB_REPETICIONES repeticiones por medición    (por defecto 5)

Salida (carpeta resultados/):
    tabla_tiempos.csv            tiempos por volumen y estrategia
    escalabilidad_nodos.csv      1, 2, 4 y 8 nodos con 500 000 registros
    particiones.json             resultado parcial de cada nodo + consolidación
    resultados_consolidados.json reporte de negocio (500 000 registros)
    entorno.json                 características del equipo de prueba
    resultados_completos.json    todo lo anterior en un solo archivo
"""

import csv
import gc
import io
import json
import math
import os
import platform
import statistics
import sys
import time
import multiprocessing as mp
from collections import defaultdict
from datetime import datetime

BASE = os.path.dirname(os.path.abspath(__file__))
CARPETA_DATOS = os.path.join(BASE, "datos")
CARPETA_RESULTADOS = os.path.join(BASE, "resultados")

# La interfaz gráfica puede ajustar estos valores con variables de entorno.
NODOS = max(1, int(os.environ.get("LAB_NODOS", "4")))
REPETICIONES = max(1, int(os.environ.get("LAB_REPETICIONES", "5")))
_SELECCION = os.environ.get("LAB_DATASET", "todos").strip().lower()
_TODOS_LOS_CONJUNTOS = [
    ("ventas_10k.csv", 10_000),
    ("ventas_100k.csv", 100_000),
    ("ventas_500k.csv", 500_000),
]
CONJUNTOS = [
    item for item in _TODOS_LOS_CONJUNTOS
    if _SELECCION == "todos" or item[0].endswith(f"_{_SELECCION}.csv")
] or _TODOS_LOS_CONJUNTOS
NODOS_ESCALABILIDAD = (1, 2, 4, 8)


# --------------------------------------------------------------------------
# Lectura y análisis de registros
# --------------------------------------------------------------------------

def analizar_texto(texto):
    """Convierte líneas CSV (sin encabezado) en tuplas tipadas.

    Cada tupla: (id_venta, fecha, ciudad, producto, cantidad, precio_unitario).
    """
    filas = []
    for fila in csv.reader(io.StringIO(texto)):
        if not fila:
            continue
        filas.append((fila[0], fila[1], fila[2], fila[3], int(fila[4]), float(fila[5])))
    return filas


def leer_csv(ruta):
    """Lectura centralizada: todo el archivo en un único proceso."""
    with open(ruta, "r", newline="", encoding="utf-8") as archivo:
        archivo.readline()  # encabezado
        return analizar_texto(archivo.read())


# --------------------------------------------------------------------------
# MAP: trabajo que ejecuta cada nodo
# --------------------------------------------------------------------------

def procesar_particion(filas):
    """Función que ejecuta CADA NODO sobre su partición de registros.

    Calcula agregados parciales: ingresos por ciudad, unidades por producto
    e ingresos por mes. Las tres operaciones son sumas, es decir, son
    conmutativas y asociativas: por eso pueden combinarse en cualquier orden.
    """
    inicio = time.perf_counter()
    ingresos_ciudad = defaultdict(float)
    unidades_producto = defaultdict(int)
    ingresos_mes = defaultdict(float)
    total_ingresos = 0.0
    total_unidades = 0

    for _id, fecha, ciudad, producto, cantidad, precio in filas:
        ingreso = cantidad * precio
        ingresos_ciudad[ciudad] += ingreso
        unidades_producto[producto] += cantidad
        ingresos_mes[fecha[:7]] += ingreso  # AAAA-MM
        total_ingresos += ingreso
        total_unidades += cantidad

    return {
        "registros": len(filas),
        "ingresos_ciudad": dict(ingresos_ciudad),
        "unidades_producto": dict(unidades_producto),
        "ingresos_mes": dict(ingresos_mes),
        "ingresos_totales": total_ingresos,
        "unidades_totales": total_unidades,
        "tiempo_calculo": time.perf_counter() - inicio,
        "pid": os.getpid(),
    }


def procesar_bloque(tarea):
    """Estrategia B: el nodo lee su propio bloque de bytes del CSV y lo agrega.

    `tarea` = (ruta, inicio_byte, fin_byte). Solo viajan por el canal entre
    procesos tres valores de entrada y un diccionario pequeño de salida.
    """
    ruta, inicio_byte, fin_byte = tarea
    t0 = time.perf_counter()
    with open(ruta, "rb") as archivo:
        archivo.seek(inicio_byte)
        datos = archivo.read(fin_byte - inicio_byte)
    filas = analizar_texto(datos.decode("utf-8"))
    parcial = procesar_particion(filas)
    parcial["tiempo_calculo"] = time.perf_counter() - t0  # incluye lectura
    parcial["bytes"] = fin_byte - inicio_byte
    return parcial


# --------------------------------------------------------------------------
# Particionamiento
# --------------------------------------------------------------------------

def particionar(filas, numero_nodos):
    """Estrategia A: divide las filas en N particiones equilibradas (round-robin)."""
    return [filas[i::numero_nodos] for i in range(numero_nodos)]


def calcular_bloques(ruta, numero_nodos):
    """Estrategia B: divide el archivo en N bloques de bytes alineados a líneas.

    Equivale a los "splits" de HDFS/Spark: cada bloque empieza justo después
    de un salto de línea, de modo que ningún registro queda partido.
    """
    tamano = os.path.getsize(ruta)
    with open(ruta, "rb") as archivo:
        archivo.readline()  # el encabezado no pertenece a ningún bloque
        inicio_datos = archivo.tell()
        limites = [inicio_datos]
        paso = (tamano - inicio_datos) / numero_nodos
        for k in range(1, numero_nodos):
            archivo.seek(int(inicio_datos + k * paso))
            archivo.readline()  # avanza hasta el siguiente fin de línea
            limites.append(min(archivo.tell(), tamano))
        limites.append(tamano)
    return [(ruta, limites[k], limites[k + 1]) for k in range(numero_nodos)]


# --------------------------------------------------------------------------
# REDUCE: consolidación
# --------------------------------------------------------------------------

def consolidar(partes):
    """Integra los resultados parciales de todos los nodos."""
    ingresos_ciudad = defaultdict(float)
    unidades_producto = defaultdict(int)
    ingresos_mes = defaultdict(float)

    for parte in partes:
        for ciudad, valor in parte["ingresos_ciudad"].items():
            ingresos_ciudad[ciudad] += valor
        for producto, unidades in parte["unidades_producto"].items():
            unidades_producto[producto] += unidades
        for mes, valor in parte["ingresos_mes"].items():
            ingresos_mes[mes] += valor

    top_cinco = sorted(unidades_producto.items(), key=lambda par: (-par[1], par[0]))[:5]

    return {
        "registros": sum(p["registros"] for p in partes),
        "ingresos_ciudad": dict(sorted(ingresos_ciudad.items())),
        "unidades_producto": dict(unidades_producto),
        "ingresos_mes": dict(sorted(ingresos_mes.items())),
        "top_cinco_productos": [
            {"producto": nombre, "unidades": unidades} for nombre, unidades in top_cinco
        ],
        "ingresos_totales": sum(p["ingresos_totales"] for p in partes),
        "unidades_totales": sum(p["unidades_totales"] for p in partes),
        "tiempo_calculo_nodos": sum(p["tiempo_calculo"] for p in partes),
    }


# --------------------------------------------------------------------------
# Ejecuciones (una medición)
# --------------------------------------------------------------------------

def ejecutar_secuencial(filas):
    """Baseline de cálculo: datos ya en memoria, un solo proceso."""
    inicio = time.perf_counter()
    resultado = consolidar([procesar_particion(filas)])
    return resultado, time.perf_counter() - inicio


def ejecutar_secuencial_completo(ruta):
    """Baseline de pipeline completo: leer + analizar + agregar en un proceso."""
    inicio = time.perf_counter()
    resultado = consolidar([procesar_particion(leer_csv(ruta))])
    return resultado, time.perf_counter() - inicio


def _costo_coordinacion(t_total, parciales):
    """Tiempo que NO es trabajo útil del nodo más lento (arranque, envío, unión)."""
    return max(0.0, t_total - max(p["tiempo_calculo"] for p in parciales))


def ejecutar_distribuido(filas, numero_nodos):
    """Estrategia A: particionar en memoria, enviar filas, consolidar."""
    inicio = time.perf_counter()
    particiones = particionar(filas, numero_nodos)
    with mp.Pool(processes=numero_nodos) as pool:
        parciales = pool.map(procesar_particion, particiones)
    resultado = consolidar(parciales)
    total = time.perf_counter() - inicio
    return resultado, total, parciales, _costo_coordinacion(total, parciales)


def ejecutar_distribuido_bloques(ruta, numero_nodos):
    """Estrategia B: cada nodo lee su bloque del archivo (localidad de datos)."""
    inicio = time.perf_counter()
    tareas = calcular_bloques(ruta, numero_nodos)
    with mp.Pool(processes=numero_nodos) as pool:
        parciales = pool.map(procesar_bloque, tareas)
    resultado = consolidar(parciales)
    total = time.perf_counter() - inicio
    return resultado, total, parciales, _costo_coordinacion(total, parciales)


# --------------------------------------------------------------------------
# Verificación y estadística
# --------------------------------------------------------------------------

def verificar(referencia, distribuido):
    """Comprueba que ambos caminos produzcan los mismos agregados.

    Los ingresos son números de punto flotante: al sumarlos en otro orden
    pueden diferir en el último decimal, por eso se compara con tolerancia
    de un centavo. Las unidades (enteros) deben ser exactamente iguales.
    """
    if referencia["registros"] != distribuido["registros"]:
        return False, "número de registros"

    def mismo_mapa(a, b, tolerancia=0.01):
        if set(a) != set(b):
            return False
        return all(math.isclose(a[k], b[k], rel_tol=1e-9, abs_tol=tolerancia) for k in a)

    for clave in ("ingresos_ciudad", "ingresos_mes"):
        if not mismo_mapa(referencia[clave], distribuido[clave]):
            return False, clave
    if referencia["unidades_producto"] != distribuido["unidades_producto"]:
        return False, "unidades_producto"
    if referencia["top_cinco_productos"] != distribuido["top_cinco_productos"]:
        return False, "top_cinco_productos"
    if not math.isclose(referencia["ingresos_totales"], distribuido["ingresos_totales"],
                        rel_tol=1e-9, abs_tol=0.01):
        return False, "ingresos_totales"
    return True, "OK"


def medir(funcion, repeticiones=REPETICIONES):
    """Ejecuta `funcion` varias veces; devuelve (último resultado, mediana, desviación).

    `funcion` debe devolver una tupla cuyo segundo elemento es el tiempo.
    """
    tiempos, salida = [], None
    for _ in range(repeticiones):
        salida = funcion()
        tiempos.append(salida[1])
    desviacion = statistics.stdev(tiempos) if len(tiempos) > 1 else 0.0
    return salida, statistics.median(tiempos), desviacion


def guardar(nombre, contenido):
    with open(os.path.join(CARPETA_RESULTADOS, nombre), "w", encoding="utf-8") as archivo:
        json.dump(contenido, archivo, ensure_ascii=False, indent=2)


def guardar_csv(nombre, filas):
    with open(os.path.join(CARPETA_RESULTADOS, nombre), "w", newline="",
              encoding="utf-8") as archivo:
        escritor = csv.DictWriter(archivo, fieldnames=list(filas[0].keys()))
        escritor.writeheader()
        escritor.writerows(filas)


def describir_entorno():
    nucleos_fisicos = None
    try:
        import psutil  # opcional
        nucleos_fisicos = psutil.cpu_count(logical=False)
    except Exception:
        pass
    return {
        "fecha_prueba": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "python": sys.version.split()[0],
        "sistema_operativo": platform.platform(),
        "procesador": platform.processor(),
        "nucleos_fisicos": nucleos_fisicos,
        "procesadores_logicos": os.cpu_count(),
        "nodos_simulados": NODOS,
        "repeticiones_por_medicion": REPETICIONES,
        "estadistico": "mediana",
        "estrategia_A": "envío de registros, round-robin (i mod N)",
        "estrategia_B": "localidad de datos, bloques de bytes alineados a línea",
        "semilla_datos": 42,
        "observacion": "Simulación local con multiprocessing; no es un clúster distribuido real.",
    }


# --------------------------------------------------------------------------
# Programa principal
# --------------------------------------------------------------------------

def main():
    os.makedirs(CARPETA_RESULTADOS, exist_ok=True)
    mp.freeze_support()
    entorno = describir_entorno()
    guardar("entorno.json", entorno)

    print(f"=== Pruebas de rendimiento ({NODOS} nodos, mediana de {REPETICIONES} "
          f"repeticiones) ===")
    tabla = []
    for nombre_archivo, cantidad in CONJUNTOS:
        ruta = os.path.join(CARPETA_DATOS, nombre_archivo)

        # Comparación 2 (primero, con la memoria del coordinador limpia para
        # que el recolector de basura no penalice al baseline): pipeline
        # completo con lectura incluida vs estrategia B.
        gc.collect()
        (seq_c, _), t_sec_c, d_sec_c = medir(lambda: ejecutar_secuencial_completo(ruta))
        (dist_b, _, _, coord_b), t_b, d_b = medir(
            lambda: ejecutar_distribuido_bloques(ruta, NODOS))

        # Comparación 1: solo cálculo (datos ya en memoria) vs estrategia A.
        filas = leer_csv(ruta)
        (seq, _), t_sec, d_sec = medir(lambda: ejecutar_secuencial(filas))
        (dist_a, _, _, coord_a), t_a, d_a = medir(lambda: ejecutar_distribuido(filas, NODOS))

        ok_a, det_a = verificar(seq, dist_a)
        ok_b, det_b = verificar(seq_c, dist_b)

        fila = {
            "archivo": nombre_archivo,
            "registros": cantidad,
            "nodos": NODOS,
            "repeticiones": REPETICIONES,
            # Estrategia A
            "t_secuencial_calculo_s": round(t_sec, 4),
            "t_secuencial_calculo_desv": round(d_sec, 4),
            "t_distribuido_A_s": round(t_a, 4),
            "t_distribuido_A_desv": round(d_a, 4),
            "aceleracion_A": round(t_sec / t_a, 3),
            "eficiencia_A": round(t_sec / t_a / NODOS, 3),
            "coordinacion_A_s": round(coord_a, 4),
            "verificacion_A": det_a,
            # Estrategia B
            "t_secuencial_completo_s": round(t_sec_c, 4),
            "t_secuencial_completo_desv": round(d_sec_c, 4),
            "t_distribuido_B_s": round(t_b, 4),
            "t_distribuido_B_desv": round(d_b, 4),
            "aceleracion_B": round(t_sec_c / t_b, 3),
            "eficiencia_B": round(t_sec_c / t_b / NODOS, 3),
            "coordinacion_B_s": round(coord_b, 4),
            "verificacion_B": det_b,
            "throughput_B_reg_s": round(cantidad / t_b),
            "resultados_iguales": ok_a and ok_b,
        }
        tabla.append(fila)
        print(f"{cantidad:>7,} reg. | A: T_sec={t_sec:6.3f}s T_dist={t_a:6.3f}s "
              f"S={fila['aceleracion_A']:5.2f}x | B: T_sec={t_sec_c:6.3f}s "
              f"T_dist={t_b:6.3f}s S={fila['aceleracion_B']:5.2f}x | "
              f"idénticos={ok_a and ok_b}")

        # Evidencia de la Fase 3: parciales por nodo vs consolidación final.
        if nombre_archivo == CONJUNTOS[-1][0]:
            _, _, parciales, _ = ejecutar_distribuido_bloques(ruta, NODOS)
            final = consolidar(parciales)
            guardar("particiones.json", {
                "archivo": nombre_archivo,
                "estrategia": "B - bloques del archivo",
                "nodos": [
                    {
                        "nodo": k + 1,
                        "pid": p["pid"],
                        "bytes": p["bytes"],
                        "registros": p["registros"],
                        "ingresos_COP": round(p["ingresos_totales"], 2),
                        "unidades": p["unidades_totales"],
                        "tiempo_s": round(p["tiempo_calculo"], 4),
                        "ingresos_ciudad_COP": {c: round(v, 2) for c, v in
                                                sorted(p["ingresos_ciudad"].items())},
                    }
                    for k, p in enumerate(parciales)
                ],
                "suma_de_parciales": {
                    "registros": sum(p["registros"] for p in parciales),
                    "ingresos_COP": round(sum(p["ingresos_totales"] for p in parciales), 2),
                    "unidades": sum(p["unidades_totales"] for p in parciales),
                },
                "consolidado_final": {
                    "registros": final["registros"],
                    "ingresos_COP": round(final["ingresos_totales"], 2),
                    "unidades": final["unidades_totales"],
                },
                "coincide_con_secuencial": verificar(seq_c, final)[0],
            })
            guardar("resultados_consolidados.json", {
                "archivo": nombre_archivo,
                "registros": final["registros"],
                "ingresos_totales_COP": round(final["ingresos_totales"], 2),
                "unidades_totales": final["unidades_totales"],
                "top_cinco_productos": final["top_cinco_productos"],
                "ingresos_por_ciudad_COP": {
                    k: round(v, 2) for k, v in final["ingresos_ciudad"].items()},
                "ingresos_por_mes_COP": {
                    k: round(v, 2) for k, v in final["ingresos_mes"].items()},
                "unidades_por_producto": dict(sorted(
                    final["unidades_producto"].items(), key=lambda x: -x[1])),
            })

        del filas  # libera memoria antes del siguiente conjunto

    guardar_csv("tabla_tiempos.csv", tabla)

    # ---------------- Escalabilidad ----------------
    nombre_esc = CONJUNTOS[-1][0]
    ruta = os.path.join(CARPETA_DATOS, nombre_esc)
    print(f"\n=== Escalabilidad: {', '.join(map(str, NODOS_ESCALABILIDAD))} nodos "
          f"({nombre_esc}) ===")
    gc.collect()
    # Estrategia B primero (memoria limpia), luego A con los datos cargados.
    _, base_b, _ = medir(lambda: ejecutar_secuencial_completo(ruta))
    medidas_b = {n: medir(lambda: ejecutar_distribuido_bloques(ruta, n))
                 for n in NODOS_ESCALABILIDAD}
    filas = leer_csv(ruta)
    _, base_a, _ = medir(lambda: ejecutar_secuencial(filas))
    escalabilidad = []
    for nodos in NODOS_ESCALABILIDAD:
        (_, _, _, coord_a), t_a, d_a = medir(lambda: ejecutar_distribuido(filas, nodos))
        (_, _, _, coord_b), t_b, d_b = medidas_b[nodos]
        fila = {
            "nodos": nodos,
            "registros": len(filas),
            "t_A_s": round(t_a, 4),
            "aceleracion_A": round(base_a / t_a, 3),
            "eficiencia_A": round(base_a / t_a / nodos, 3),
            "coordinacion_A_s": round(coord_a, 4),
            "t_B_s": round(t_b, 4),
            "desv_B": round(d_b, 4),
            "aceleracion_B": round(base_b / t_b, 3),
            "eficiencia_B": round(base_b / t_b / nodos, 3),
            "coordinacion_B_s": round(coord_b, 4),
            # Escalabilidad relativa: compara contra la misma estrategia con 1 nodo.
            "aceleracion_relativa_B": round(medidas_b[1][1] / t_b, 3),
            "eficiencia_relativa_B": round(medidas_b[1][1] / t_b / nodos, 3),
        }
        escalabilidad.append(fila)
        print(f"{nodos} nodo(s) | A: T={t_a:6.3f}s S={fila['aceleracion_A']:5.2f}x | "
              f"B: T={t_b:6.3f}s S={fila['aceleracion_B']:5.2f}x "
              f"E={fila['eficiencia_B']:4.2f}")
    guardar_csv("escalabilidad_nodos.csv", escalabilidad)

    guardar("resultados_completos.json", {
        "entorno": entorno,
        "linea_base_escalabilidad": {
            "t_secuencial_calculo_s": round(base_a, 4),
            "t_secuencial_completo_s": round(base_b, 4),
        },
        "tabla_tiempos": tabla,
        "escalabilidad": escalabilidad,
    })
    print("\nResultados guardados en:", CARPETA_RESULTADOS)


if __name__ == "__main__":
    main()
