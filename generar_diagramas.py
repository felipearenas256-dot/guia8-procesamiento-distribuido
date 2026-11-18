"""Genera los diagramas y gráficas del informe (PNG de alta resolución).

Todas las cifras se leen de la carpeta resultados/: ningún valor está escrito
a mano, así que las figuras siempre coinciden con la última ejecución.

Uso:
    python generar_diagramas.py

Salida: diagramas/*.png
"""

import json
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

BASE = os.path.dirname(os.path.abspath(__file__))
SALIDA = os.path.join(BASE, "diagramas")
CARPETA_RES = os.path.join(BASE, "resultados")
os.makedirs(SALIDA, exist_ok=True)


def leer(nombre):
    ruta = os.path.join(CARPETA_RES, nombre)
    if not os.path.exists(ruta):
        return None
    with open(ruta, encoding="utf-8") as f:
        return json.load(f)


RESULTADOS = leer("resultados_completos.json")
INTENSIVO = leer("escenario_intensivo.json")
CONSOLIDADO = leer("resultados_consolidados.json")
PARTICIONES = leer("particiones.json")
FALLOS = leer("tolerancia_fallos.json")
SPARK = leer("spark_resultados.json")

AZUL = "#1F3B63"
AZUL_MEDIO = "#3B6EA8"
AZUL_CLARO = "#DCE8F7"
NARANJA = "#E9A23B"
NARANJA_CLARO = "#FCEBCF"
VERDE = "#2E7D6B"
VERDE_CLARO = "#D6EFE8"
MORADO = "#6C5BA5"
MORADO_CLARO = "#E7E1F5"
GRIS = "#5B6B7C"
GRIS_CLARO = "#EEF2F6"
ROJO = "#C0392B"
ROJO_CLARO = "#FBE0DC"

plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 10,
    "axes.edgecolor": "#B8C4D4",
    "axes.labelcolor": AZUL,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "text.color": AZUL,
    "xtick.color": GRIS,
    "ytick.color": GRIS,
})


def es(numero, decimales=3):
    """Formato numérico en español (coma decimal)."""
    return f"{numero:.{decimales}f}".replace(".", ",")


def miles(numero):
    return f"{numero:,.0f}".replace(",", " ")


def guardar(fig, nombre):
    ruta = os.path.join(SALIDA, nombre)
    fig.savefig(ruta, dpi=200, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print("Generado:", ruta)


def caja(ax, x, y, w, h, texto, fc, ec, fontsize=9.5, bold=True, color=None):
    ax.add_patch(FancyBboxPatch(
        (x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.10",
        linewidth=1.5, edgecolor=ec, facecolor=fc, zorder=2))
    ax.text(x + w / 2, y + h / 2, texto, ha="center", va="center",
            fontsize=fontsize, fontweight="bold" if bold else "normal",
            color=color or AZUL, zorder=3, linespacing=1.35)


def numero(ax, x, y, n, color=AZUL):
    """Círculo numerado que vincula el diagrama con los 6 elementos de la guía."""
    ax.add_patch(plt.Circle((x, y), 0.19, color=color, zorder=4))
    ax.text(x, y, str(n), ha="center", va="center", color="white",
            fontsize=9, fontweight="bold", zorder=5)


def flecha(ax, p1, p2, color=GRIS, rad=0.0, lw=1.7, estilo="-|>"):
    ax.add_patch(FancyArrowPatch(
        p1, p2, arrowstyle=estilo, mutation_scale=15, linewidth=lw, color=color,
        connectionstyle=f"arc3,rad={rad}", zorder=1))


# ---------------------------------------------------------------------------
# Figura 1: arquitectura
# ---------------------------------------------------------------------------

def arquitectura():
    nodos = RESULTADOS["entorno"]["nodos_simulados"] if RESULTADOS else 4
    fig, ax = plt.subplots(figsize=(13.5, 8.6))
    ax.set_xlim(0, 13.5)
    ax.set_ylim(0, 8.6)
    ax.axis("off")

    ax.text(6.75, 8.3, "Arquitectura de procesamiento distribuido",
            ha="center", fontsize=17, fontweight="bold")
    ax.text(6.75, 7.92, f"{nodos} nodos simulados (procesos)  ·  patrón Map / Reduce  ·  "
            "los números corresponden a los 6 elementos exigidos en la Fase 2",
            ha="center", fontsize=10, color=GRIS)

    # 1 y 2: fuente y particionador
    caja(ax, 0.4, 6.35, 3.5, 1.05, "FUENTE DE DATOS (CSV)\nventas_10k / 100k / 500k",
         AZUL_CLARO, AZUL, 10)
    numero(ax, 0.45, 7.38, 1)
    caja(ax, 4.85, 6.35, 3.8, 1.05,
         "PARTICIONADOR (coordinador)\nA: filas i mod N  ·  B: bloques de bytes",
         NARANJA_CLARO, NARANJA, 9.6)
    numero(ax, 4.9, 7.38, 2, NARANJA)
    caja(ax, 9.6, 6.35, 3.5, 1.05, "BASELINE SECUENCIAL\n1 proceso, sin partición",
         GRIS_CLARO, GRIS, 10)
    flecha(ax, (3.9, 6.87), (4.85, 6.87), AZUL, lw=2)
    flecha(ax, (8.65, 6.87), (9.6, 6.87), GRIS, lw=1.3)

    # 3: nodos
    xs = [0.35, 3.65, 6.95, 10.25]
    for i, x in enumerate(xs, start=1):
        etiqueta = f"NODO {i}" if i < 4 or nodos == 4 else f"NODO … {nodos}"
        caja(ax, x, 4.3, 2.9, 1.25,
             f"{etiqueta}\nlee su bloque → agrega\n(ciudad · producto · mes)",
             VERDE_CLARO, VERDE, 9)
        flecha(ax, (6.75, 6.35), (x + 1.45, 5.55), NARANJA, lw=1.5)
    numero(ax, 0.4, 5.53, 3, VERDE)
    ax.text(6.75, 3.98, "MAP: cada nodo produce agregados parciales "
            "(diccionarios pequeños)", ha="center", fontsize=9, color=VERDE,
            fontweight="bold", zorder=6,
            bbox=dict(boxstyle="round,pad=0.25", fc="white", ec="none"))

    # 4: consolidación
    caja(ax, 4.4, 2.35, 4.7, 1.2,
         "CONSOLIDADOR (Reduce)\nsuma de parciales · top 5 · verificación",
         MORADO_CLARO, MORADO, 10)
    numero(ax, 4.45, 3.53, 4, MORADO)
    for x in xs:
        flecha(ax, (x + 1.45, 4.3), (6.75, 3.55), VERDE, lw=1.5)

    # 5 y 6: salidas
    salidas = [
        (0.5, "REPORTES / VISUALIZACIONES\ningresos por ciudad y mes\ntop 5 productos", AZUL_CLARO, AZUL, 5),
        (5.0, "TOLERANCIA A FALLOS\ntimeout · reintento ·\ncheckpoints", ROJO_CLARO, ROJO, None),
        (9.5, "REGISTRO DE PRUEBAS\ntiempos, aceleración\nresultados/*.json · *.csv", GRIS_CLARO, GRIS, 6),
    ]
    for x, texto, fc, ec, n in salidas:
        caja(ax, x, 0.45, 3.5, 1.2, texto, fc, ec, 9)
        flecha(ax, (6.75, 2.35), (x + 1.75, 1.65), MORADO, lw=1.5)
        if n:
            numero(ax, x + 0.05, 1.63, n, ec)
    guardar(fig, "01_arquitectura.png")


# ---------------------------------------------------------------------------
# Figura 2: tiempos por volumen, estrategias A y B
# ---------------------------------------------------------------------------

def tiempos():
    filas = RESULTADOS["tabla_tiempos"]
    n = filas[0]["nodos"]
    etiquetas = [f"{f['registros'] // 1000} mil" for f in filas]
    x = range(len(filas))
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.9), sharey=False)

    paneles = [
        (ax1, "t_secuencial_calculo_s", "t_distribuido_A_s", "aceleracion_A",
         "A · Envío de registros (solo cálculo)", ROJO),
        (ax2, "t_secuencial_completo_s", "t_distribuido_B_s", "aceleracion_B",
         "B · Localidad de datos (lectura + cálculo)", VERDE),
    ]
    for ax, k_sec, k_dist, k_s, titulo, color_s in paneles:
        sec = [f[k_sec] for f in filas]
        dist = [f[k_dist] for f in filas]
        b1 = ax.bar([i - 0.19 for i in x], sec, 0.38, label="Secuencial (1 proceso)",
                    color=AZUL)
        b2 = ax.bar([i + 0.19 for i in x], dist, 0.38, label=f"Distribuido ({n} nodos)",
                    color=NARANJA)
        ax.set_yscale("log")
        ax.set_title(titulo, fontsize=11.5, fontweight="bold")
        ax.set_xticks(list(x), etiquetas)
        ax.set_xlabel("Registros procesados")
        ax.grid(axis="y", which="both", alpha=0.25)
        ax.set_axisbelow(True)
        ax.set_ylim(min(sec + dist) * 0.5, max(sec + dist) * 4)
        for barras in (b1, b2):
            for barra in barras:
                ax.annotate(es(barra.get_height()),
                            (barra.get_x() + barra.get_width() / 2, barra.get_height()),
                            xytext=(0, 3), textcoords="offset points", ha="center",
                            fontsize=7.8, color=GRIS)
        texto_s = "   ".join(f"{e}: S={es(f[k_s], 2)}×" for e, f in zip(etiquetas, filas))
        ax.text(0.5, 0.94, texto_s, transform=ax.transAxes, ha="center", fontsize=8.6,
                color=color_s, fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.35", fc="white", ec=color_s, alpha=0.9))
    ax1.set_ylabel("Tiempo (s, escala log, mediana)")
    manejadores, textos = ax1.get_legend_handles_labels()
    fig.legend(manejadores, textos, frameon=False, loc="lower center", ncol=2,
               fontsize=9.5, bbox_to_anchor=(0.5, -0.06))
    fig.suptitle("Tiempo de ejecución: secuencial vs distribuido", fontsize=13.5,
                 fontweight="bold", y=1.02)
    fig.tight_layout()
    guardar(fig, "02_tiempos.png")


# ---------------------------------------------------------------------------
# Figura 3: escalabilidad
# ---------------------------------------------------------------------------

def escalabilidad():
    filas = RESULTADOS["escalabilidad"]
    nodos = [f["nodos"] for f in filas]
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11.5, 4.5))

    ax1.plot(nodos, [f["t_A_s"] for f in filas], marker="o", lw=2.4, color=ROJO,
             label="A · envío de registros")
    ax1.plot(nodos, [f["t_B_s"] for f in filas], marker="s", lw=2.4, color=VERDE,
             label="B · localidad de datos")
    for f in filas:
        ax1.annotate(f"{es(f['t_B_s'], 2)} s", (f["nodos"], f["t_B_s"]), xytext=(0, -15),
                     textcoords="offset points", ha="center", fontsize=8.5, color=VERDE)
        ax1.annotate(f"{es(f['t_A_s'], 2)} s", (f["nodos"], f["t_A_s"]), xytext=(0, 8),
                     textcoords="offset points", ha="center", fontsize=8.5, color=ROJO)
    ax1.set_title("Tiempo vs. número de nodos", fontsize=11.5, fontweight="bold")
    ax1.set_xlabel("Nodos simulados")
    ax1.set_ylabel("Tiempo (s, mediana)")
    ax1.set_xscale("log", base=2)
    ax1.set_xticks(nodos, [str(n) for n in nodos])
    ax1.set_ylim(0, max(f["t_A_s"] for f in filas) * 1.25)
    ax1.grid(alpha=0.25)
    ax1.legend(frameon=False, fontsize=8.5)

    rel = [f["aceleracion_relativa_B"] for f in filas]
    ax2.plot(nodos, nodos, ls="--", color="#9AA8BC", lw=1.6, label="Ideal (S = N)")
    ax2.plot(nodos, rel, marker="s", lw=2.4, color=VERDE, label="B · S relativa = T(1)/T(N)")
    for n, s, f in zip(nodos, rel, filas):
        ax2.annotate(f"S={es(s, 2)}\nE={es(f['eficiencia_relativa_B'], 2)}", (n, s),
                     xytext=(0, 10), textcoords="offset points", ha="center",
                     fontsize=8.3, color=VERDE)
    ax2.set_title("Aceleración relativa de la estrategia B", fontsize=11.5,
                  fontweight="bold")
    ax2.set_xlabel("Nodos simulados")
    ax2.set_ylabel("Aceleración")
    ax2.set_xscale("log", base=2)
    ax2.set_xticks(nodos, [str(n) for n in nodos])
    ax2.set_ylim(-0.8, max(nodos) + 0.5)
    ax2.grid(alpha=0.25)
    ax2.legend(frameon=False, fontsize=8.5, loc="upper left")

    fig.suptitle(f"Escalabilidad con {miles(filas[0]['registros'])} registros",
                 fontsize=13.5, fontweight="bold", y=1.02)
    fig.tight_layout()
    guardar(fig, "03_escalabilidad.png")


# ---------------------------------------------------------------------------
# Figura 4: escenario intensivo
# ---------------------------------------------------------------------------

def intensivo():
    sec = INTENSIVO["tiempo_secuencial_s"]
    dist = INTENSIVO["tiempo_distribuido_s"]
    n = INTENSIVO["nodos"]
    fig, ax = plt.subplots(figsize=(8.2, 4.8))
    nombres = ["Secuencial\n(1 proceso)", f"Distribuido\n({n} nodos)", f"Ideal\n(T_s / {n})"]
    valores = [sec, dist, sec / n]
    barras = ax.bar(nombres, valores, 0.58, color=[AZUL, NARANJA, "#B7C4D3"])
    for barra, valor in zip(barras, valores):
        ax.annotate(f"{es(valor)} s", (barra.get_x() + barra.get_width() / 2, valor),
                    xytext=(0, 6), textcoords="offset points", ha="center",
                    fontsize=10, fontweight="bold")
    ax.set_ylabel("Tiempo de ejecución (s, mediana)")
    ax.set_title(f"Escenario CPU-intensivo: {miles(INTENSIVO['registros'])} registros · "
                 f"{INTENSIVO['iteraciones_por_registro']} iteraciones/registro",
                 fontsize=12, fontweight="bold")
    ax.grid(axis="y", alpha=0.25)
    ax.set_axisbelow(True)
    ax.set_ylim(0, sec * 1.25)
    ax.text(0.98, 0.95,
            f"Aceleración real: {es(INTENSIVO['aceleracion'])}×\n"
            f"Eficiencia: {es(INTENSIVO['eficiencia'])}\n"
            f"Coordinación: {es(INTENSIVO['costo_coordinacion_s'])} s",
            transform=ax.transAxes, ha="right", va="top", fontsize=9.5,
            bbox=dict(boxstyle="round,pad=0.45", fc="#EAF6F2", ec=VERDE))
    fig.tight_layout()
    guardar(fig, "04_intensivo.png")


# ---------------------------------------------------------------------------
# Figura 5: tolerancia a fallos (flujo + resultados medidos)
# ---------------------------------------------------------------------------

def tolerancia():
    fig = plt.figure(figsize=(13, 5.6))
    ax = fig.add_axes([0.0, 0.0, 0.52, 1.0])
    ax.set_xlim(0, 7)
    ax.set_ylim(0, 6)
    ax.axis("off")
    falla = FALLOS["nodo_que_falla"] if FALLOS else 3
    nodos = FALLOS["nodos"] if FALLOS else 4
    ax.text(3.5, 5.6, "Mecanismo de recuperación", ha="center", fontsize=13,
            fontweight="bold")
    ancho = 6.2 / nodos
    for i in range(1, nodos + 1):
        x = 0.4 + (i - 1) * ancho
        cae = i == falla
        caja(ax, x, 4.2, ancho - 0.2, 0.9,
             f"NODO {i}\n{'FALLA ✕' if cae else 'OK ✓'}",
             ROJO_CLARO if cae else VERDE_CLARO, ROJO if cae else VERDE, 9,
             color=ROJO if cae else AZUL)
    pasos = [
        ("1. DETECTAR", "excepción del proceso o\ntimeout (heartbeat perdido)"),
        ("2. CONSERVAR", "las particiones correctas\nquedan en checkpoint"),
        ("3. REEJECUTAR", "solo la partición fallida\n(tarea idempotente)"),
        ("4. VERIFICAR", "registros consolidados\n= registros esperados"),
    ]
    for k, (titulo, texto) in enumerate(pasos):
        y = 3.15 - k * 0.85
        caja(ax, 0.6, y, 5.8, 0.68, f"{titulo}:  {texto.replace(chr(10), ' ')}",
             AZUL_CLARO, AZUL, 8.6)
        if k:
            flecha(ax, (3.5, y + 0.85), (3.5, y + 0.68), AZUL, lw=1.4)
    flecha(ax, (0.4 + (falla - 0.5) * ancho - 0.1, 4.2), (3.5, 3.83), ROJO, lw=1.6)

    if FALLOS:
        ax2 = fig.add_axes([0.58, 0.16, 0.4, 0.7])
        esc = FALLOS["escenarios"]
        nombres = [e["escenario"].replace(" (sin fallos)", "").replace(" a fallos", "")
                   .replace(" + ", "\n+ ") for e in esc]
        pct = [e["registros_consolidados"] / e["registros_esperados"] * 100 for e in esc]
        colores = [VERDE if e["resultado_correcto"] else ROJO for e in esc]
        barras = ax2.bar(nombres, pct, 0.6, color=colores)
        for barra, e in zip(barras, esc):
            ax2.annotate(
                f"{es(barra.get_height(), 1)} %\n{es(e['tiempo_total_s'], 2)} s",
                (barra.get_x() + barra.get_width() / 2, barra.get_height()),
                xytext=(0, 4), textcoords="offset points", ha="center", fontsize=8.5,
                color=colores[esc.index(e)], fontweight="bold")
        ax2.set_ylim(0, 125)
        ax2.set_ylabel("Registros consolidados (%)")
        ax2.set_title(f"Resultado medido ({FALLOS['archivo']}, falla el nodo {falla})",
                      fontsize=11, fontweight="bold")
        ax2.axhline(100, ls="--", color="#9AA8BC", lw=1)
        ax2.tick_params(axis="x", labelsize=8.3)
        sin = next((e for e in esc if not e["tolerante"]), None)
        if sin:
            ax2.text(0.5, -0.24, f"Sin tolerancia se pierden {miles(sin['registros_perdidos'])} "
                     f"registros y los ingresos quedan subestimados {es(sin['error_ingresos_pct'], 1)} %",
                     transform=ax2.transAxes, ha="center", fontsize=9, color=ROJO,
                     fontweight="bold")
    guardar(fig, "05_tolerancia_fallos.png")


# ---------------------------------------------------------------------------
# Figura 6: reporte de negocio (Fase 2, elemento 5)
# ---------------------------------------------------------------------------

def negocio():
    c = CONSOLIDADO
    fig = plt.figure(figsize=(13, 8.2))
    rejilla = fig.add_gridspec(2, 2, height_ratios=[1, 1], hspace=0.42, wspace=0.28)

    ax1 = fig.add_subplot(rejilla[0, 0])
    ciudades = sorted(c["ingresos_por_ciudad_COP"].items(), key=lambda x: x[1])
    ax1.barh([k for k, _ in ciudades], [v / 1e9 for _, v in ciudades], color=AZUL_MEDIO)
    for i, (_, v) in enumerate(ciudades):
        ax1.text(v / 1e9, i, f" {es(v / 1e9, 1)}", va="center", fontsize=8.3, color=GRIS)
    ax1.set_title("Ingresos por ciudad (miles de millones COP)", fontsize=11,
                  fontweight="bold")
    ax1.set_xlim(min(v for _, v in ciudades) / 1e9 * 0.95,
                 max(v for _, v in ciudades) / 1e9 * 1.02)
    ax1.grid(axis="x", alpha=0.25)

    ax2 = fig.add_subplot(rejilla[0, 1])
    top = c["top_cinco_productos"][::-1]
    ax2.barh([p["producto"] for p in top], [p["unidades"] for p in top], color=NARANJA)
    for i, p in enumerate(top):
        ax2.text(p["unidades"], i, f" {miles(p['unidades'])}", va="center", fontsize=8.3,
                 color=GRIS)
    ax2.set_title("Top 5 de productos por unidades vendidas", fontsize=11,
                  fontweight="bold")
    ax2.set_xlim(0, top[-1]["unidades"] * 1.2)
    ax2.grid(axis="x", alpha=0.25)

    ax3 = fig.add_subplot(rejilla[1, :])
    meses = list(c["ingresos_por_mes_COP"].items())
    nombres_mes = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct",
                   "Nov", "Dic"]
    valores = [v / 1e9 for _, v in meses]
    maximo = max(valores)
    colores = [VERDE if v >= sorted(valores)[-2] else "#9CC9BC" for v in valores]
    ax3.bar([nombres_mes[int(m[5:]) - 1] for m, _ in meses], valores, color=colores)
    for i, v in enumerate(valores):
        ax3.text(i, v, f"{es(v, 1)}", ha="center", va="bottom", fontsize=8.3, color=GRIS)
    ax3.set_title("Ingresos por mes, 2025 (miles de millones COP) · en verde los dos "
                  "meses de mayor actividad", fontsize=11, fontweight="bold")
    ax3.set_ylim(0, maximo * 1.15)
    ax3.grid(axis="y", alpha=0.25)

    fig.suptitle(f"Reporte consolidado · {miles(c['registros'])} ventas · ingresos totales "
                 f"COP {es(c['ingresos_totales_COP'] / 1e12, 3)} billones",
                 fontsize=13.5, fontweight="bold", y=0.99)
    guardar(fig, "06_reporte_ventas.png")


# ---------------------------------------------------------------------------
# Figura 7: parciales por nodo vs consolidado (Fase 3, comparación)
# ---------------------------------------------------------------------------

def particiones():
    p = PARTICIONES
    nodos = p["nodos"]
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.6),
                                   gridspec_kw={"width_ratios": [1.3, 1]})
    colores = [AZUL_MEDIO, NARANJA, VERDE, MORADO, GRIS, ROJO, "#8C6D31", "#4A9BB5"]
    etiquetas = [f"Nodo {n['nodo']}" for n in nodos]
    ax1.bar(etiquetas, [n["registros"] for n in nodos],
            color=colores[:len(nodos)])
    for i, n in enumerate(nodos):
        ax1.text(i, n["registros"], f"{miles(n['registros'])}\n{es(n['tiempo_s'], 3)} s",
                 ha="center", va="bottom", fontsize=8.5, color=GRIS)
    ax1.set_ylim(0, max(n["registros"] for n in nodos) * 1.25)
    ax1.set_title("Registros procesados por cada nodo", fontsize=11, fontweight="bold")
    ax1.grid(axis="y", alpha=0.25)

    izquierda = 0
    for i, n in enumerate(nodos):
        valor = n["ingresos_COP"] / 1e9
        ax2.barh(["Suma de\nparciales"], [valor], left=izquierda, color=colores[i],
                 label=etiquetas[i])
        izquierda += valor
    total = p["consolidado_final"]["ingresos_COP"] / 1e9
    ax2.barh(["Consolidado\nfinal"], [total], color=AZUL)
    ax2.text(total, 1, f" {es(total, 2)}", va="center", fontsize=9, fontweight="bold")
    ax2.text(izquierda, 0, f" {es(izquierda, 2)}", va="center", fontsize=9,
             fontweight="bold")
    ax2.set_xlim(0, total * 1.28)
    ax2.set_xlabel("Ingresos (miles de millones COP)")
    ax2.set_title("Parciales vs consolidación", fontsize=11, fontweight="bold")
    ax2.legend(frameon=False, fontsize=8, loc="upper center", ncol=4,
               bbox_to_anchor=(0.5, -0.2))
    marca = "✓ coincide con el secuencial" if p["coincide_con_secuencial"] else "✗ difiere"
    fig.suptitle(f"Fase 3 · {p['archivo']} repartido en {len(nodos)} nodos  ·  {marca}",
                 fontsize=13, fontweight="bold", y=1.03)
    fig.tight_layout()
    guardar(fig, "07_particiones.png")


# ---------------------------------------------------------------------------
# Figura 8: Apache Spark (opcional)
# ---------------------------------------------------------------------------

def spark():
    filas = SPARK["tabla"]
    etiquetas = [f"{f['registros'] / 1e6:g} M" if f["registros"] >= 1e6
                 else f"{f['registros'] // 1000} mil" for f in filas]
    x = list(range(len(filas)))
    series = [
        ("Python secuencial", "t_python_secuencial_s", AZUL),
        ("Python B · 4 procesos", "t_python_bloques_B_s", VERDE),
        (f"Apache Spark {SPARK['version_spark']} · {SPARK['master']}", "t_spark_s", NARANJA),
    ]
    fig, ax = plt.subplots(figsize=(10, 4.8))
    ancho = 0.26
    for k, (nombre, clave, color) in enumerate(series):
        valores = [f.get(clave, 0) for f in filas]
        barras = ax.bar([i + (k - 1) * ancho for i in x], valores, ancho, color=color,
                        label=nombre)
        for barra in barras:
            ax.annotate(es(barra.get_height(), 2),
                        (barra.get_x() + barra.get_width() / 2, barra.get_height()),
                        xytext=(0, 3), textcoords="offset points", ha="center",
                        fontsize=7.8, color=GRIS)
    ax.set_xticks(x, etiquetas)
    ax.set_xlabel("Registros procesados")
    ax.set_ylabel("Tiempo de consulta (s, mediana)")
    ax.set_title("Python secuencial vs Python distribuido (B) vs Apache Spark (modo local)",
                 fontsize=12, fontweight="bold")
    ax.grid(axis="y", alpha=0.25)
    ax.set_axisbelow(True)
    maximo = max(f.get(c, 0) for f in filas for _, c, _ in series)
    ax.set_ylim(0, maximo * 1.22)
    ax.legend(frameon=False, loc="upper left", fontsize=9)
    ax.text(0.99, 0.97, f"Arranque de la JVM, no incluido: {es(SPARK['arranque_sesion_s'], 1)} s",
            transform=ax.transAxes, ha="right", va="top", fontsize=8.5, color=GRIS)
    fig.tight_layout()
    guardar(fig, "08_spark.png")


if __name__ == "__main__":
    arquitectura()
    if RESULTADOS:
        tiempos()
        escalabilidad()
    if INTENSIVO:
        intensivo()
    tolerancia()
    if CONSOLIDADO:
        negocio()
    if PARTICIONES:
        particiones()
    if SPARK:
        spark()
    print("Diagramas listos en:", SALIDA)
