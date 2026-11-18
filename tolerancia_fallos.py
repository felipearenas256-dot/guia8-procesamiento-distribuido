"""Guía 8 - Fase 5, pregunta 4: ¿qué pasa si un nodo falla antes de entregar?

Este script NO se queda en la teoría: provoca fallos reales en un nodo y mide
qué ocurre con y sin mecanismos de tolerancia a fallos.

Escenarios (todos con la estrategia B, bloques del archivo):

  0. Normal               : ningún nodo falla (referencia).
  1. Sin tolerancia       : el nodo F lanza una excepción; el coordinador
                            consolida solo lo que recibió.  -> resultado sesgado
  2. Caída + reintento    : el coordinador detecta la excepción y reejecuta
                            SOLO la partición fallida (tarea idempotente).
                            Las particiones correctas quedan en checkpoint.
  3. Bloqueo + timeout    : el nodo F se cuelga (no responde). El coordinador
                            lo detecta por tiempo de espera (equivalente a un
                            heartbeat perdido) y reasigna la partición a otro
                            proceso del pool.

Uso:
    python tolerancia_fallos.py

Variables de entorno:
    LAB_NODOS        número de nodos (por defecto 4)
    LAB_FALLO_NODO   nodo que falla, 1..N (por defecto 3)
    LAB_DATASET      10k | 100k | 500k (por defecto 100k)
    LAB_ESCENARIO    todos | 0 | 1 | 2 | 3 (por defecto todos)
    LAB_RETARDO_DEMO segundos extra por nodo SOLO para la animación de la
                     interfaz (por defecto 0; no se usa en las mediciones)

Las líneas que empiezan con "@@EVENTO" son mensajes JSON que la interfaz
gráfica usa para animar el estado de cada nodo; en consola pueden ignorarse.

Salida: resultados/tolerancia_fallos.json y resultados/checkpoints/*.json
"""

import json
import os
import shutil
import sys
import time
import multiprocessing as mp

from procesamiento_distribuido import (
    CARPETA_DATOS, CARPETA_RESULTADOS, calcular_bloques, consolidar,
    ejecutar_secuencial_completo, procesar_bloque, verificar,
)

NODOS = max(2, int(os.environ.get("LAB_NODOS", "4")))
FALLO_NODO = min(NODOS, max(1, int(os.environ.get("LAB_FALLO_NODO", "3"))))
_DS = os.environ.get("LAB_DATASET", "100k").strip().lower()
DATASET = f"ventas_{_DS if _DS in ('10k', '100k', '500k') else '100k'}.csv"
ESCENARIO = os.environ.get("LAB_ESCENARIO", "todos").strip().lower()
RETARDO_DEMO = float(os.environ.get("LAB_RETARDO_DEMO", "0"))
TIMEOUT_S = 3.0 + RETARDO_DEMO * 2     # espera máxima por nodo antes de declararlo caído
MAX_REINTENTOS = 3
CARPETA_CHECKPOINTS = os.path.join(CARPETA_RESULTADOS, "checkpoints")


def evento(**datos):
    """Emite un evento legible por la interfaz gráfica."""
    print("@@EVENTO " + json.dumps(datos, ensure_ascii=False), flush=True)


def tarea_nodo(args):
    """Trabajo de un nodo con inyección de fallos controlada.

    args = (nodo, intento, bloque, modo_fallo, nodo_que_falla, retardo)
    El fallo solo se inyecta en el PRIMER intento: así se comprueba que el
    reintento de la misma partición produce el resultado correcto.
    """
    nodo, intento, bloque, modo_fallo, nodo_falla, retardo = args
    if retardo:
        time.sleep(retardo)
    if nodo == nodo_falla and intento == 1:
        if modo_fallo == "excepcion":
            raise RuntimeError(f"Nodo {nodo}: fallo simulado (proceso caído)")
        if modo_fallo == "bloqueo":
            time.sleep(3600)  # deja de responder; el coordinador usará timeout
    parcial = procesar_bloque(bloque)
    parcial["nodo"] = nodo
    parcial["intento"] = intento
    return parcial


def guardar_checkpoint(parcial):
    """Persiste el resultado parcial de un nodo (checkpoint)."""
    ruta = os.path.join(CARPETA_CHECKPOINTS, f"nodo_{parcial['nodo']}.json")
    with open(ruta, "w", encoding="utf-8") as archivo:
        json.dump(parcial, archivo, ensure_ascii=False)


def ejecutar_escenario(nombre, ruta, modo_fallo, tolerante):
    """Ejecuta un escenario y devuelve sus métricas."""
    shutil.rmtree(CARPETA_CHECKPOINTS, ignore_errors=True)
    os.makedirs(CARPETA_CHECKPOINTS, exist_ok=True)
    nodo_falla = FALLO_NODO if modo_fallo else 0
    bloques = calcular_bloques(ruta, NODOS)
    evento(tipo="escenario", nombre=nombre, nodos=NODOS, falla=nodo_falla,
           tolerante=tolerante)

    inicio = time.perf_counter()
    parciales, fallidos, bitacora = {}, [], []
    # Un proceso extra en el pool permite reasignar trabajo aunque un nodo quede colgado.
    pool = mp.Pool(processes=NODOS + (1 if tolerante else 0))
    try:
        pendientes = {}
        for nodo, bloque in enumerate(bloques, start=1):
            pendientes[nodo] = pool.apply_async(
                tarea_nodo, ((nodo, 1, bloque, modo_fallo, nodo_falla, RETARDO_DEMO),))
            evento(tipo="nodo", nodo=nodo, estado="procesando", intento=1)

        # Detección: se espera a cada nodo con un tiempo límite.
        limite = time.perf_counter() + TIMEOUT_S
        for nodo, asincrono in pendientes.items():
            try:
                parcial = asincrono.get(timeout=max(0.05, limite - time.perf_counter()))
                parciales[nodo] = parcial
                guardar_checkpoint(parcial)
                evento(tipo="nodo", nodo=nodo, estado="ok", registros=parcial["registros"])
            except mp.TimeoutError:
                fallidos.append(nodo)
                bitacora.append(f"Nodo {nodo}: sin respuesta tras {TIMEOUT_S:.1f} s (timeout)")
                evento(tipo="nodo", nodo=nodo, estado="caido", motivo="timeout")
            except Exception as error:
                fallidos.append(nodo)
                bitacora.append(str(error))
                evento(tipo="nodo", nodo=nodo, estado="caido", motivo="excepción")

        t_deteccion = time.perf_counter() - inicio

        # Recuperación: reejecutar SOLO las particiones perdidas.
        reintentos = 0
        if tolerante:
            for nodo in list(fallidos):
                for intento in range(2, MAX_REINTENTOS + 2):
                    reintentos += 1
                    evento(tipo="nodo", nodo=nodo, estado="reintentando", intento=intento)
                    try:
                        parcial = pool.apply_async(
                            tarea_nodo,
                            ((nodo, intento, bloques[nodo - 1], modo_fallo, nodo_falla,
                              RETARDO_DEMO),),
                        ).get(timeout=TIMEOUT_S)
                        parciales[nodo] = parcial
                        guardar_checkpoint(parcial)
                        fallidos.remove(nodo)
                        bitacora.append(f"Nodo {nodo}: partición recuperada en el intento "
                                        f"{intento} (PID {parcial['pid']})")
                        evento(tipo="nodo", nodo=nodo, estado="recuperado",
                               registros=parcial["registros"])
                        break
                    except Exception as error:
                        bitacora.append(f"Nodo {nodo}: intento {intento} falló ({error})")
    finally:
        pool.terminate()  # elimina también un posible proceso colgado
        pool.join()

    resultado = consolidar(list(parciales.values()))
    t_total = time.perf_counter() - inicio
    return resultado, {
        "escenario": nombre,
        "modo_fallo": modo_fallo or "ninguno",
        "tolerante": tolerante,
        "nodo_que_falla": nodo_falla or None,
        "nodos_entregados": len(parciales),
        "particiones_perdidas": len(fallidos),
        "reintentos": reintentos if tolerante else 0,
        "tiempo_total_s": round(t_total, 4),
        "tiempo_deteccion_s": round(t_deteccion, 4),
        "checkpoints": sorted(os.listdir(CARPETA_CHECKPOINTS)),
        "bitacora": bitacora,
    }


def main():
    mp.freeze_support()
    os.makedirs(CARPETA_RESULTADOS, exist_ok=True)
    ruta = os.path.join(CARPETA_DATOS, DATASET)
    referencia, _ = ejecutar_secuencial_completo(ruta)

    escenarios = [
        ("0", "Normal (sin fallos)", None, True),
        ("1", "Sin tolerancia a fallos", "excepcion", False),
        ("2", "Caída + reintento", "excepcion", True),
        ("3", "Bloqueo + timeout", "bloqueo", True),
    ]
    if ESCENARIO != "todos":
        escenarios = [e for e in escenarios if e[0] == ESCENARIO] or escenarios

    print(f"=== Tolerancia a fallos: {DATASET}, {NODOS} nodos, falla el nodo {FALLO_NODO} ===")
    filas = []
    for _id, nombre, modo, tolerante in escenarios:
        resultado, metricas = ejecutar_escenario(nombre, ruta, modo, tolerante)
        correcto, detalle = verificar(referencia, resultado)
        perdidos = referencia["registros"] - resultado["registros"]
        error_ingresos = (referencia["ingresos_totales"] - resultado["ingresos_totales"]) \
            / referencia["ingresos_totales"] * 100
        metricas.update({
            "registros_esperados": referencia["registros"],
            "registros_consolidados": resultado["registros"],
            "registros_perdidos": perdidos,
            "error_ingresos_pct": round(error_ingresos, 2),
            "resultado_correcto": correcto,
            "detalle_verificacion": detalle,
            "top5_obtenido": [p["producto"] for p in resultado["top_cinco_productos"]],
        })
        filas.append(metricas)
        estado = "CORRECTO" if correcto else f"INCORRECTO ({detalle})"
        print(f"[{nombre:<24}] T={metricas['tiempo_total_s']:6.3f}s | "
              f"registros {resultado['registros']:>7,}/{referencia['registros']:,} | "
              f"error ingresos {error_ingresos:6.2f}% | reintentos "
              f"{metricas['reintentos']} | {estado}")
        for linea in metricas["bitacora"]:
            print("    ·", linea)
        evento(tipo="fin_escenario", nombre=nombre, correcto=correcto,
               perdidos=perdidos, error=round(error_ingresos, 2),
               tiempo=metricas["tiempo_total_s"])

    salida = {
        "archivo": DATASET,
        "nodos": NODOS,
        "nodo_que_falla": FALLO_NODO,
        "timeout_s": TIMEOUT_S,
        "max_reintentos": MAX_REINTENTOS,
        "top5_referencia": [p["producto"] for p in referencia["top_cinco_productos"]],
        "escenarios": filas,
    }
    if RETARDO_DEMO == 0 and ESCENARIO == "todos":
        with open(os.path.join(CARPETA_RESULTADOS, "tolerancia_fallos.json"), "w",
                  encoding="utf-8") as archivo:
            json.dump(salida, archivo, ensure_ascii=False, indent=2)
        print("\nResultados guardados en: resultados/tolerancia_fallos.json")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
