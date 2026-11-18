"""Interfaz gráfica v3 - Guía 8 · Procesamiento Distribuido de Datos.

Novedades frente a la v2:
  * Barra lateral con el pipeline del laboratorio y el estado de cada paso.
  * Panel con indicadores y GRÁFICAS NATIVAS interactivas (pase el mouse
    sobre una barra o punto para ver su valor); se redibujan al redimensionar.
  * Simulador de tolerancia a fallos EN VIVO: elija el nodo que falla y el
    tipo de fallo, y observe cómo cada nodo procesa, cae y se recupera.
  * Resultados de las dos estrategias (A: envío de registros, B: localidad
    de datos), escalabilidad 1-8 nodos, parciales por nodo, fallos y Spark.
  * Explorador de datos con búsqueda y visor de figuras.

Uso:
    python interfaz_grafica.py

Requiere solo la biblioteca estándar de Python (tkinter).
"""

import csv
import importlib.util
import json
import math
import os
import queue
import subprocess
import sys
import threading
import time
import tkinter as tk
from tkinter import messagebox, ttk

BASE = os.path.dirname(os.path.abspath(__file__))
RESULTADOS = os.path.join(BASE, "resultados")
DIAGRAMAS = os.path.join(BASE, "diagramas")
DATOS = os.path.join(BASE, "datos")
HAY_SPARK = importlib.util.find_spec("pyspark") is not None

# (archivo, título, descripción, opcional)
PASOS = [
    ("generar_datos.py", "Datos sintéticos", "Fase 1 · 10k, 100k y 500k ventas", False),
    ("procesamiento_distribuido.py", "Procesamiento y pruebas", "Fases 3-4 · estrategias A y B", False),
    ("experimento_intensivo.py", "Escenario intensivo", "Carga CPU-bound", False),
    ("tolerancia_fallos.py", "Tolerancia a fallos", "Fase 5 · caída, reintento, timeout", False),
    ("procesamiento_spark.py", "Apache Spark", "PySpark local vs Python", True),
    ("generar_diagramas.py", "Figuras", "8 gráficas del informe", False),
    ("generar_documento.py", "Informe DOCX", "Documento para entregar", False),
    ("exportar_pdf.py", "Informe PDF", "Requiere Microsoft Word", True),
]

TEMAS = {
    "claro": {
        "bg": "#F3F5F9", "card": "#FFFFFF", "borde": "#E1E6EE", "texto": "#1B2433",
        "muted": "#6A7586", "sidebar": "#14213D", "sidebar_txt": "#E8EDF5",
        "sidebar_muted": "#8C9AB3", "sidebar_hover": "#1F2F52",
        "primario": "#2F6FDE", "primario_hover": "#2559B5", "acento": "#1F9D7A",
        "naranja": "#E59A2F", "rojo": "#D2483C", "morado": "#6C5BA5",
        "grid": "#E9EDF3", "consola_bg": "#0E1525", "consola_fg": "#D3DEEE",
        "tab": "#E7ECF3",
    },
    "oscuro": {
        "bg": "#10151F", "card": "#18202D", "borde": "#28334A", "texto": "#E6ECF5",
        "muted": "#93A1B7", "sidebar": "#0A0F18", "sidebar_txt": "#E6ECF5",
        "sidebar_muted": "#7D8BA3", "sidebar_hover": "#172133",
        "primario": "#4C8BF5", "primario_hover": "#6A9FF7", "acento": "#34C08F",
        "naranja": "#F0AE4A", "rojo": "#F06A5E", "morado": "#9A89D6",
        "grid": "#243046", "consola_bg": "#080C14", "consola_fg": "#D3DEEE",
        "tab": "#1C2535",
    },
}

ICONO_ESTADO = {"pendiente": "○", "ejecutando": "◐", "ok": "●", "error": "✕", "omitido": "–"}
FUENTE = "Segoe UI"


def fmt(numero, decimales=3):
    return f"{numero:,.{decimales}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def leer_json(nombre):
    ruta = os.path.join(RESULTADOS, nombre)
    try:
        with open(ruta, encoding="utf-8") as archivo:
            return json.load(archivo)
    except Exception:
        return None


# ===========================================================================
# Gráfica nativa (barras agrupadas o líneas) con tooltip
# ===========================================================================

class Grafica(tk.Canvas):
    def __init__(self, padre, colores, titulo="", alto=250, **kw):
        super().__init__(padre, height=alto, highlightthickness=0,
                         background=colores["card"], **kw)
        self.c = colores
        self.titulo = titulo
        self.tipo = "barras"
        self.categorias, self.series = [], []
        self.unidad = ""
        self.ideal = None
        self.zonas = []
        self.bind("<Configure>", lambda e: self.dibujar())
        self.bind("<Motion>", self._hover)
        self.bind("<Leave>", lambda e: self.delete("tip"))

    def datos(self, categorias, series, tipo="barras", unidad="", ideal=None):
        """series = [(nombre, color, [valores])]"""
        self.categorias, self.series = categorias, series
        self.tipo, self.unidad, self.ideal = tipo, unidad, ideal
        self.dibujar()

    def dibujar(self):
        self.delete("all")
        self.zonas = []
        c = self.c
        ancho, alto = self.winfo_width(), self.winfo_height()
        if ancho < 50:
            return
        self.create_text(14, 14, text=self.titulo, anchor="nw", fill=c["texto"],
                         font=(FUENTE, 10, "bold"))
        if not self.series:
            self.create_text(ancho / 2, alto / 2, text="Sin datos: ejecute el laboratorio",
                             fill=c["muted"], font=(FUENTE, 9))
            return
        # Leyenda
        x = 14
        leyenda = [(n, col, False) for n, col, _ in self.series]
        if self.ideal:
            leyenda.append(("Ideal (S = N)", c["muted"], True))
        for nombre, color, punteada in leyenda:
            if punteada:
                self.create_line(x, 42, x + 14, 42, fill=color, dash=(4, 3), width=2)
            else:
                self.create_rectangle(x, 37, x + 10, 47, fill=color, outline="")
            t = self.create_text(x + 16 + (4 if punteada else 0), 42, text=nombre,
                                 anchor="w", fill=c["muted"], font=(FUENTE, 8))
            x = self.bbox(t)[2] + 16
        izq, der, arr, aba = 52, ancho - 16, 64, alto - 30
        valores = [v for _, _, vs in self.series for v in vs]
        if self.ideal:
            valores += self.ideal
        maximo = max(valores) * 1.15 or 1
        # Rejilla
        for k in range(5):
            y = aba - (aba - arr) * k / 4
            self.create_line(izq, y, der, y, fill=c["grid"])
            self.create_text(izq - 6, y, text=fmt(maximo * k / 4, 2), anchor="e",
                             fill=c["muted"], font=(FUENTE, 7))
        n = len(self.categorias)
        paso = (der - izq) / max(n, 1)
        escala = lambda v: aba - (aba - arr) * v / maximo
        for i, cat in enumerate(self.categorias):
            self.create_text(izq + paso * (i + 0.5), aba + 12, text=cat,
                             fill=c["muted"], font=(FUENTE, 8))
        if self.tipo == "barras":
            grupo = paso * 0.7
            ancho_barra = grupo / len(self.series)
            for s, (nombre, color, vs) in enumerate(self.series):
                for i, v in enumerate(vs):
                    x0 = izq + paso * i + (paso - grupo) / 2 + s * ancho_barra
                    x1 = x0 + ancho_barra - 3
                    y0 = escala(v)
                    self.create_rectangle(x0, y0, x1, aba, fill=color, outline="")
                    self.create_text((x0 + x1) / 2, y0 - 7, text=fmt(v, 2),
                                     fill=c["muted"], font=(FUENTE, 7))
                    self.zonas.append((x0, y0, x1, aba, f"{nombre} · {self.categorias[i]}: "
                                       f"{fmt(v, 4)}{self.unidad}"))
        else:
            if self.ideal:
                puntos = [(izq + paso * (i + 0.5), escala(v)) for i, v in enumerate(self.ideal)]
                self.create_line(*sum(puntos, ()), fill=c["muted"], dash=(4, 3), width=1.5)
            for nombre, color, vs in self.series:
                puntos = [(izq + paso * (i + 0.5), escala(v)) for i, v in enumerate(vs)]
                if len(puntos) > 1:
                    self.create_line(*sum(puntos, ()), fill=color, width=2.6, smooth=False)
                for (px, py), v, cat in zip(puntos, vs, self.categorias):
                    self.create_oval(px - 4.5, py - 4.5, px + 4.5, py + 4.5, fill=color,
                                     outline=c["card"], width=2)
                    self.zonas.append((px - 9, py - 9, px + 9, py + 9,
                                       f"{nombre} · {cat}: {fmt(v, 3)}{self.unidad}"))

    def _hover(self, evento):
        self.delete("tip")
        for x0, y0, x1, y1, texto in self.zonas:
            if x0 <= evento.x <= x1 and y0 <= evento.y <= y1:
                t = self.create_text(evento.x + 12, evento.y - 12, text=texto, anchor="sw",
                                     fill="white", font=(FUENTE, 8, "bold"), tags="tip")
                x0b, y0b, x1b, y1b = self.bbox(t)
                if x1b > self.winfo_width() - 4:
                    self.move(t, -(x1b - x0b) - 24, 0)
                    x0b, y0b, x1b, y1b = self.bbox(t)
                r = self.create_rectangle(x0b - 6, y0b - 4, x1b + 6, y1b + 4,
                                          fill="#1B2433", outline="", tags="tip")
                self.tag_lower(r, t)
                return


# ===========================================================================
# Aplicación
# ===========================================================================

class Aplicacion:
    def __init__(self, root):
        self.root = root
        root.title("Guía 8 · Laboratorio de Procesamiento Distribuido")
        root.geometry("1360x880")
        root.minsize(1150, 740)

        self.cola = queue.Queue()
        self.activo = False
        self.proceso = None
        self.fotos = []
        self.figuras = []
        self.indice_figura = 0
        self.estado_pasos = {p[0]: ("omitido" if p[0] == "procesamiento_spark.py"
                                    and not HAY_SPARK else "pendiente") for p in PASOS}
        self.tema = tk.StringVar(value="claro")
        self.nodos = tk.IntVar(value=4)
        self.dataset = tk.StringVar(value="todos")
        self.repeticiones = tk.IntVar(value=5)
        self.sim_fallo = tk.IntVar(value=3)
        self.sim_modo = tk.StringVar(value="Caída + reintento")
        self.sim_dataset = tk.StringVar(value="500k")
        self.texto_consola = []

        self._construir()
        root.after(80, self._drenar_cola)
        root.protocol("WM_DELETE_WINDOW", self._salir)
        root.bind("<F5>", lambda e: self._ejecutar_todo())
        root.bind("<Control-l>", lambda e: self._limpiar_consola())

    # ------------------------------------------------------------------ estilos
    @property
    def c(self):
        return TEMAS[self.tema.get()]

    def _estilos(self):
        c = self.c
        s = ttk.Style()
        try:
            s.theme_use("clam")
        except tk.TclError:
            pass
        s.configure(".", background=c["bg"], foreground=c["texto"], font=(FUENTE, 10),
                    bordercolor=c["borde"], troughcolor=c["tab"])
        s.configure("TFrame", background=c["bg"])
        s.configure("Card.TFrame", background=c["card"])
        s.configure("TLabel", background=c["bg"], foreground=c["texto"])
        s.configure("Card.TLabel", background=c["card"], foreground=c["texto"])
        s.configure("CardMuted.TLabel", background=c["card"], foreground=c["muted"],
                    font=(FUENTE, 9))
        s.configure("Muted.TLabel", background=c["bg"], foreground=c["muted"],
                    font=(FUENTE, 9))
        s.configure("H1.TLabel", background=c["bg"], foreground=c["texto"],
                    font=(FUENTE, 16, "bold"))
        s.configure("H2.TLabel", background=c["bg"], foreground=c["texto"],
                    font=(FUENTE, 11, "bold"))
        s.configure("CardH2.TLabel", background=c["card"], foreground=c["texto"],
                    font=(FUENTE, 11, "bold"))
        s.configure("TButton", padding=(10, 6), background=c["card"], foreground=c["texto"],
                    bordercolor=c["borde"], font=(FUENTE, 9))
        s.map("TButton", background=[("active", c["tab"])])
        s.configure("Primario.TButton", background=c["primario"], foreground="white",
                    bordercolor=c["primario"], font=(FUENTE, 10, "bold"), padding=(12, 9))
        s.map("Primario.TButton", background=[("active", c["primario_hover"]),
                                               ("disabled", c["muted"])])
        s.configure("Peligro.TButton", background=c["rojo"], foreground="white",
                    bordercolor=c["rojo"], font=(FUENTE, 9, "bold"))
        s.map("Peligro.TButton", background=[("active", c["rojo"])])
        s.configure("TNotebook", background=c["bg"], borderwidth=0, tabmargins=(0, 4, 0, 0))
        s.configure("TNotebook.Tab", background=c["bg"], foreground=c["muted"],
                    padding=(16, 8), font=(FUENTE, 10, "bold"), borderwidth=0)
        s.map("TNotebook.Tab", background=[("selected", c["card"])],
              foreground=[("selected", c["primario"])])
        s.configure("Treeview", background=c["card"], fieldbackground=c["card"],
                    foreground=c["texto"], rowheight=26, font=(FUENTE, 9), borderwidth=0)
        s.configure("Treeview.Heading", background=c["tab"], foreground=c["texto"],
                    font=(FUENTE, 9, "bold"), relief="flat", padding=(4, 6))
        s.map("Treeview", background=[("selected", c["primario"])],
              foreground=[("selected", "white")])
        s.configure("TCombobox", fieldbackground=c["card"], background=c["card"],
                    foreground=c["texto"], arrowcolor=c["texto"])
        s.map("TCombobox", fieldbackground=[("readonly", c["card"])],
              foreground=[("readonly", c["texto"])],
              selectbackground=[("readonly", c["card"])],
              selectforeground=[("readonly", c["texto"])])
        s.configure("TSpinbox", fieldbackground=c["card"], background=c["card"],
                    foreground=c["texto"], arrowcolor=c["texto"])
        s.configure("Horizontal.TProgressbar", background=c["acento"],
                    troughcolor=c["tab"], thickness=6, borderwidth=0)
        s.configure("TRadiobutton", background=c["card"], foreground=c["texto"])
        self.root.configure(bg=c["bg"])

    # ---------------------------------------------------------- construcción
    def _construir(self):
        self._estilos()
        for hijo in self.root.winfo_children():
            hijo.destroy()
        self.contenedor = tk.Frame(self.root, bg=self.c["bg"])
        self.contenedor.pack(fill="both", expand=True)
        self._barra_lateral()
        principal = ttk.Frame(self.contenedor, padding=(18, 14, 18, 8))
        principal.pack(side="left", fill="both", expand=True)

        cabecera = ttk.Frame(principal)
        cabecera.pack(fill="x")
        ttk.Label(cabecera, text="Procesamiento distribuido de ventas",
                  style="H1.TLabel").pack(side="left")
        ttk.Label(cabecera, text="  simulación con procesos locales · no es un clúster real",
                  style="Muted.TLabel").pack(side="left", pady=(6, 0))

        self.cuaderno = ttk.Notebook(principal)
        self.cuaderno.pack(fill="both", expand=True, pady=(10, 0))
        self._tab_panel()
        self._tab_simulador()
        self._tab_resultados()
        self._tab_datos()
        self._tab_figuras()
        self._tab_consola()

        pie = ttk.Frame(principal)
        pie.pack(fill="x", pady=(8, 0))
        self.lbl_estado = ttk.Label(pie, text="Listo", style="Muted.TLabel")
        self.lbl_estado.pack(side="left")
        self.progreso = ttk.Progressbar(pie, length=260, mode="determinate")
        self.progreso.pack(side="right")
        self._refrescar_todo()

    def _barra_lateral(self):
        c = self.c
        lateral = tk.Frame(self.contenedor, bg=c["sidebar"], width=290)
        lateral.pack(side="left", fill="y")
        lateral.pack_propagate(False)

        tk.Label(lateral, text="GUÍA 8", bg=c["sidebar"], fg=c["sidebar_muted"],
                 font=(FUENTE, 8, "bold")).pack(anchor="w", padx=20, pady=(18, 0))
        tk.Label(lateral, text="Laboratorio distribuido", bg=c["sidebar"],
                 fg=c["sidebar_txt"], font=(FUENTE, 14, "bold")).pack(anchor="w", padx=20)

        # Configuración
        cfg = tk.Frame(lateral, bg=c["sidebar"])
        cfg.pack(fill="x", padx=20, pady=(16, 6))
        for fila, (texto, widget) in enumerate([
            ("Nodos", ttk.Spinbox(cfg, from_=1, to=8, width=6, textvariable=self.nodos)),
            ("Datos", ttk.Combobox(cfg, width=8, state="readonly", textvariable=self.dataset,
                                   values=["todos", "10k", "100k", "500k"])),
            ("Repeticiones", ttk.Spinbox(cfg, from_=1, to=15, width=6,
                                         textvariable=self.repeticiones)),
        ]):
            tk.Label(cfg, text=texto, bg=c["sidebar"], fg=c["sidebar_muted"],
                     font=(FUENTE, 9)).grid(row=fila, column=0, sticky="w", pady=3)
            widget.grid(row=fila, column=1, sticky="e", pady=3)
        cfg.columnconfigure(1, weight=1)

        self.btn_todo = ttk.Button(lateral, text="▶  Ejecutar todo  (F5)",
                                   style="Primario.TButton", command=self._ejecutar_todo)
        self.btn_todo.pack(fill="x", padx=20, pady=(8, 4))
        self.btn_detener = ttk.Button(lateral, text="■  Detener", style="Peligro.TButton",
                                      command=self._detener)
        self.btn_detener.pack(fill="x", padx=20, pady=(0, 10))
        self.btn_detener.state(["disabled"])

        inferior = tk.Frame(lateral, bg=c["sidebar"])
        inferior.pack(side="bottom", fill="x", padx=20, pady=(6, 14))
        for texto, accion in [
            ("◐  Cambiar tema", self._alternar_tema),
            ("↗  Carpeta de resultados", lambda: self._abrir(RESULTADOS)),
            ("↗  Abrir informe", self._abrir_informe),
        ]:
            b = tk.Label(inferior, text=texto, bg=c["sidebar"], fg=c["sidebar_txt"],
                         font=(FUENTE, 9), cursor="hand2", anchor="w")
            b.pack(fill="x", pady=2)
            b.bind("<Button-1>", lambda e, a=accion: a())

        tk.Label(lateral, text="PIPELINE · clic para ejecutar un paso", bg=c["sidebar"],
                 fg=c["sidebar_muted"], font=(FUENTE, 8, "bold")).pack(anchor="w", padx=20,
                                                                    pady=(6, 4))
        self.filas_pasos = {}
        for numero, (archivo, titulo, desc, _) in enumerate(PASOS, start=1):
            fila = tk.Frame(lateral, bg=c["sidebar"], cursor="hand2")
            fila.pack(fill="x", padx=12, pady=1)
            icono = tk.Label(fila, width=2, bg=c["sidebar"], font=(FUENTE, 12))
            icono.pack(side="left", padx=(6, 4))
            textos = tk.Frame(fila, bg=c["sidebar"])
            textos.pack(side="left", fill="x", expand=True, pady=2)
            t1 = tk.Label(textos, text=f"{numero}. {titulo}", bg=c["sidebar"],
                          fg=c["sidebar_txt"], font=(FUENTE, 9, "bold"), anchor="w")
            t1.pack(fill="x")
            t2 = tk.Label(textos, text=desc, bg=c["sidebar"], fg=c["sidebar_muted"],
                          font=(FUENTE, 8), anchor="w")
            t2.pack(fill="x")
            widgets = (fila, icono, textos, t1, t2)
            for w in widgets:
                w.bind("<Button-1>", lambda e, a=archivo: self._ejecutar([a]))
                w.bind("<Enter>", lambda e, ws=widgets: [x.configure(bg=self.c["sidebar_hover"])
                                                        for x in ws])
                w.bind("<Leave>", lambda e, ws=widgets: [x.configure(bg=self.c["sidebar"])
                                                        for x in ws])
            self.filas_pasos[archivo] = icono
            self._pintar_paso(archivo)

    def _pintar_paso(self, archivo):
        estado = self.estado_pasos[archivo]
        colores = {"pendiente": self.c["sidebar_muted"], "ejecutando": self.c["naranja"],
                   "ok": self.c["acento"], "error": self.c["rojo"],
                   "omitido": self.c["sidebar_muted"]}
        icono = self.filas_pasos.get(archivo)
        if icono and icono.winfo_exists():
            icono.configure(text=ICONO_ESTADO[estado], fg=colores[estado])

    def _tarjeta(self, padre, **pack):
        marco = tk.Frame(padre, bg=self.c["card"], highlightthickness=1,
                         highlightbackground=self.c["borde"])
        marco.pack(**pack)
        return marco

    # ------------------------------------------------------------------ Panel
    def _tab_panel(self):
        marco = ttk.Frame(self.cuaderno, padding=(2, 12, 2, 2))
        self.cuaderno.add(marco, text="Panel")
        fila = ttk.Frame(marco)
        fila.pack(fill="x")
        self.kpis = {}
        defin = [
            ("registros", "Registros", self.c["primario"]),
            ("acel_b", "Aceleración B", self.c["acento"]),
            ("acel_a", "Aceleración A", self.c["rojo"]),
            ("fallos", "Error si falla", self.c["naranja"]),
            ("verif", "Verificación", self.c["morado"]),
        ]
        for i, (clave, titulo, color) in enumerate(defin):
            t = tk.Frame(fila, bg=self.c["card"], highlightthickness=1,
                         highlightbackground=self.c["borde"])
            t.grid(row=0, column=i, sticky="nsew", padx=(0 if i == 0 else 10, 0))
            fila.columnconfigure(i, weight=1, uniform="kpi")
            tk.Frame(t, bg=color, height=3).pack(fill="x")
            tk.Label(t, text=titulo.upper(), bg=self.c["card"], fg=self.c["muted"],
                     font=(FUENTE, 8, "bold")).pack(anchor="w", padx=14, pady=(10, 0))
            valor = tk.Label(t, text="—", bg=self.c["card"], fg=self.c["texto"],
                             font=(FUENTE, 20, "bold"))
            valor.pack(anchor="w", padx=14)
            sub = tk.Label(t, text="", bg=self.c["card"], fg=self.c["muted"],
                           font=(FUENTE, 8), justify="left", wraplength=150)
            sub.pack(anchor="w", padx=14, pady=(0, 10))
            self.kpis[clave] = (valor, sub)

        graficas = ttk.Frame(marco)
        graficas.pack(fill="both", expand=True, pady=(12, 0))
        graficas.columnconfigure(0, weight=1)
        graficas.columnconfigure(1, weight=1)
        graficas.rowconfigure(0, weight=1)
        graficas.rowconfigure(1, weight=1)
        self.g_acel = Grafica(graficas, self.c, "Aceleración S = Ts / Tp por volumen")
        self.g_esc = Grafica(graficas, self.c, "Escalabilidad (500 mil): aceleración relativa B")
        self.g_tiempo = Grafica(graficas, self.c, "Tiempo con N nodos (s)")
        self.g_fallos = Grafica(graficas, self.c, "Registros consolidados por escenario de fallo (%)")
        for g, (r, col) in zip((self.g_acel, self.g_esc, self.g_tiempo, self.g_fallos),
                               [(0, 0), (0, 1), (1, 0), (1, 1)]):
            g.configure(highlightthickness=1, highlightbackground=self.c["borde"])
            g.grid(row=r, column=col, sticky="nsew", padx=(0 if col == 0 else 6,
                                                          6 if col == 0 else 0), pady=4)

    def _refrescar_panel(self):
        completo = leer_json("resultados_completos.json")
        fallos = leer_json("tolerancia_fallos.json")
        consolidado = leer_json("resultados_consolidados.json")
        c = self.c
        if consolidado:
            self.kpis["registros"][0].configure(text=fmt(consolidado["registros"], 0))
            self.kpis["registros"][1].configure(
                text=f"COP {fmt(consolidado['ingresos_totales_COP'] / 1e12, 3)} billones")
        if completo:
            tabla = completo["tabla_tiempos"]
            ultimo = tabla[-1]
            self.kpis["acel_b"][0].configure(text=f"{fmt(ultimo['aceleracion_B'], 2)}×")
            self.kpis["acel_b"][1].configure(text=f"localidad de datos · {ultimo['nodos']} nodos")
            self.kpis["acel_a"][0].configure(text=f"{fmt(ultimo['aceleracion_A'], 2)}×")
            self.kpis["acel_a"][1].configure(text=f"envío de filas · {ultimo['nodos']} nodos")
            todos = all(f["resultados_iguales"] for f in tabla)
            self.kpis["verif"][0].configure(text="✓ Exacta" if todos else "✕ Difiere",
                                            fg=c["acento"] if todos else c["rojo"])
            self.kpis["verif"][1].configure(text="distribuido = secuencial")
            cats = [f"{f['registros'] // 1000} mil" for f in tabla]
            self.g_acel.datos(cats, [("A · envío de filas", c["rojo"],
                                      [f["aceleracion_A"] for f in tabla]),
                                     ("B · localidad", c["acento"],
                                      [f["aceleracion_B"] for f in tabla])], unidad="×")
            esc = completo["escalabilidad"]
            cats_n = [f"{f['nodos']} nodos" for f in esc]
            self.g_esc.datos(cats_n, [("B real", c["acento"],
                                       [f["aceleracion_relativa_B"] for f in esc])],
                             tipo="lineas", unidad="×", ideal=[f["nodos"] for f in esc])
            self.g_tiempo.datos(cats_n, [("A", c["rojo"], [f["t_A_s"] for f in esc]),
                                         ("B", c["acento"], [f["t_B_s"] for f in esc])],
                                tipo="lineas", unidad=" s")
        if fallos:
            esc = fallos["escenarios"]
            sin = next((e for e in esc if not e["tolerante"]), None)
            if sin:
                self.kpis["fallos"][0].configure(text=f"{fmt(sin['error_ingresos_pct'], 1)} %")
                self.kpis["fallos"][1].configure(text="sin tolerancia (0 % con reintento)")
            self.g_fallos.datos(
                [e["escenario"].replace(" (sin fallos)", "").replace(" a fallos", "")
                 for e in esc],
                [("Consolidado", c["primario"],
                  [e["registros_consolidados"] / e["registros_esperados"] * 100 for e in esc])],
                unidad=" %")

    # -------------------------------------------------------------- Simulador
    def _tab_simulador(self):
        marco = ttk.Frame(self.cuaderno, padding=(2, 12, 2, 2))
        self.cuaderno.add(marco, text="Simulador de fallos")
        c = self.c

        controles = self._tarjeta(marco, fill="x")
        interior = tk.Frame(controles, bg=c["card"])
        interior.pack(fill="x", padx=16, pady=12)
        tk.Label(interior, text="¿Qué pasa si un nodo falla antes de entregar su resultado?",
                 bg=c["card"], fg=c["texto"], font=(FUENTE, 11, "bold")).grid(
            row=0, column=0, columnspan=8, sticky="w")
        tk.Label(interior, text="Se ejecuta tolerancia_fallos.py con un retardo visual de "
                 "0,8 s por nodo para poder seguir la animación.",
                 bg=c["card"], fg=c["muted"], font=(FUENTE, 9)).grid(
            row=1, column=0, columnspan=8, sticky="w", pady=(0, 10))
        tk.Label(interior, text="Escenario", bg=c["card"], fg=c["muted"]).grid(row=2, column=0)
        ttk.Combobox(interior, textvariable=self.sim_modo, state="readonly", width=24,
                     values=["Normal (sin fallos)", "Sin tolerancia a fallos",
                             "Caída + reintento", "Bloqueo + timeout"]).grid(
            row=2, column=1, padx=(6, 18))
        tk.Label(interior, text="Nodo que falla", bg=c["card"], fg=c["muted"]).grid(
            row=2, column=2)
        ttk.Spinbox(interior, from_=1, to=8, width=5, textvariable=self.sim_fallo).grid(
            row=2, column=3, padx=(6, 18))
        tk.Label(interior, text="Datos", bg=c["card"], fg=c["muted"]).grid(row=2, column=4)
        ttk.Combobox(interior, textvariable=self.sim_dataset, state="readonly", width=7,
                     values=["10k", "100k", "500k"]).grid(row=2, column=5, padx=(6, 18))
        ttk.Button(interior, text="▶  Simular", style="Primario.TButton",
                   command=self._simular).grid(row=2, column=6)

        self.lienzo_nodos = tk.Canvas(marco, height=330, bg=c["card"], highlightthickness=1,
                                      highlightbackground=c["borde"])
        self.lienzo_nodos.pack(fill="x", pady=12)
        self.lienzo_nodos.bind("<Configure>", lambda e: self._dibujar_nodos())
        self.estado_nodos = {}
        self.sim_info = {"nodos": self.nodos.get(), "titulo": "Configure y pulse «Simular»"}

        self.sim_resultado = self._tarjeta(marco, fill="both", expand=True)
        self.lbl_sim = tk.Label(self.sim_resultado, text="El resultado de la verificación "
                                "aparecerá aquí.", bg=c["card"], fg=c["muted"],
                                font=(FUENTE, 10), justify="left", anchor="nw")
        self.lbl_sim.pack(fill="both", expand=True, padx=16, pady=12)

    def _dibujar_nodos(self):
        lz = self.lienzo_nodos
        if not lz.winfo_exists():
            return
        lz.delete("all")
        c = self.c
        ancho = lz.winfo_width()
        n = max(1, self.sim_info.get("nodos", 4))
        lz.create_text(20, 18, text=self.sim_info.get("titulo", ""), anchor="nw",
                       fill=c["texto"], font=(FUENTE, 11, "bold"))
        # Coordinador
        cx = ancho / 2
        lz.create_rectangle(cx - 130, 50, cx + 130, 96, fill=c["tab"], outline=c["borde"])
        lz.create_text(cx, 73, text="COORDINADOR · particiona en bloques", fill=c["texto"],
                       font=(FUENTE, 9, "bold"))
        estilos = {
            "espera": (c["tab"], c["muted"], "en espera"),
            "procesando": (c["naranja"], "white", "procesando…"),
            "ok": (c["acento"], "white", "entregado ✓"),
            "caido": (c["rojo"], "white", "CAÍDO ✕"),
            "reintentando": (c["morado"], "white", "reintentando…"),
            "recuperado": (c["primario"], "white", "recuperado ✓"),
        }
        margen = 30
        w = min(170, (ancho - 2 * margen) / n - 16)
        total_w = n * w + (n - 1) * 16
        x0 = (ancho - total_w) / 2
        for i in range(1, n + 1):
            x = x0 + (i - 1) * (w + 16)
            estado = self.estado_nodos.get(i, {}).get("estado", "espera")
            fondo, texto, etiqueta = estilos.get(estado, estilos["espera"])
            lz.create_line(cx, 96, x + w / 2, 140, fill=c["borde"], width=2, arrow="last")
            lz.create_rectangle(x, 140, x + w, 222, fill=fondo, outline="")
            lz.create_text(x + w / 2, 162, text=f"NODO {i}", fill=texto,
                           font=(FUENTE, 11, "bold"))
            lz.create_text(x + w / 2, 186, text=etiqueta, fill=texto, font=(FUENTE, 9))
            extra = self.estado_nodos.get(i, {})
            if extra.get("registros"):
                lz.create_text(x + w / 2, 206, text=f"{fmt(extra['registros'], 0)} reg.",
                               fill=texto, font=(FUENTE, 8))
            elif extra.get("motivo"):
                lz.create_text(x + w / 2, 206, text=extra["motivo"], fill=texto,
                               font=(FUENTE, 8))
            color_linea = c["rojo"] if estado == "caido" else c["borde"]
            lz.create_line(x + w / 2, 222, cx, 266, fill=color_linea, width=2, arrow="last",
                           dash=(4, 3) if estado == "caido" else None)
        lz.create_rectangle(cx - 130, 266, cx + 130, 312, fill=c["tab"], outline=c["borde"])
        lz.create_text(cx, 289, text="CONSOLIDADOR · suma + verificación", fill=c["texto"],
                       font=(FUENTE, 9, "bold"))

    def _simular(self):
        if self.activo:
            messagebox.showwarning("Proceso en curso", "Espere a que termine la tarea actual.")
            return
        escenario = {"Normal (sin fallos)": "0", "Sin tolerancia a fallos": "1",
                     "Caída + reintento": "2", "Bloqueo + timeout": "3"}[self.sim_modo.get()]
        n = max(2, self.nodos.get())
        self.estado_nodos = {}
        self.sim_info = {"nodos": n, "titulo": self.sim_modo.get()}
        self._dibujar_nodos()
        self.lbl_sim.configure(text="Simulando…", fg=self.c["muted"])
        extra = {"LAB_ESCENARIO": escenario, "LAB_FALLO_NODO": str(self.sim_fallo.get()),
                 "LAB_DATASET": self.sim_dataset.get(), "LAB_RETARDO_DEMO": "0.8",
                 "LAB_NODOS": str(n)}
        self._ejecutar(["tolerancia_fallos.py"], extra_env=extra, marcar=False)

    def _evento(self, datos):
        tipo = datos.get("tipo")
        if tipo == "escenario":
            self.sim_info = {"nodos": datos["nodos"], "titulo": datos["nombre"] +
                             (f" · falla el nodo {datos['falla']}" if datos.get("falla") else "")}
            self.estado_nodos = {}
        elif tipo == "nodo":
            self.estado_nodos[datos["nodo"]] = datos
        elif tipo == "fin_escenario":
            ok = datos["correcto"]
            if not ok:
                explicacion = ("Sin detección ni reintento, el consolidador sumó solo las "
                               "particiones recibidas: los agregados quedan subestimados "
                               "y nadie recibe un aviso.")
            elif datos["nombre"].startswith("Normal"):
                explicacion = "Todos los nodos entregaron su partición a tiempo."
            else:
                explicacion = ("El coordinador detectó el fallo y reejecutó solo la partición "
                               "perdida; las demás se conservaron en checkpoint.")
            self.lbl_sim.configure(
                fg=self.c["acento"] if ok else self.c["rojo"],
                text=(("✓ RESULTADO CORRECTO" if ok else "✕ RESULTADO INCORRECTO") + "\n\n"
                      f"Registros perdidos: {fmt(datos['perdidos'], 0)}\n"
                      f"Error en ingresos totales: {fmt(datos['error'], 2)} %\n"
                      f"Tiempo total: {fmt(datos['tiempo'], 2)} s (incluye retardo visual)\n\n"
                      f"{explicacion}"))
        self._dibujar_nodos()

    # ------------------------------------------------------------- Resultados
    def _tab_resultados(self):
        marco = ttk.Frame(self.cuaderno, padding=(2, 12, 2, 2))
        self.cuaderno.add(marco, text="Resultados")
        lienzo = tk.Canvas(marco, bg=self.c["bg"], highlightthickness=0)
        barra = ttk.Scrollbar(marco, orient="vertical", command=lienzo.yview)
        interior = ttk.Frame(lienzo)
        interior.bind("<Configure>", lambda e: lienzo.configure(scrollregion=lienzo.bbox("all")))
        ventana = lienzo.create_window((0, 0), window=interior, anchor="nw")
        lienzo.bind("<Configure>", lambda e: lienzo.itemconfigure(ventana, width=e.width))
        lienzo.configure(yscrollcommand=barra.set)
        lienzo.pack(side="left", fill="both", expand=True)
        barra.pack(side="right", fill="y")
        lienzo.bind_all("<MouseWheel>", lambda e: lienzo.yview_scroll(-e.delta // 120, "units")
                        if self.cuaderno.index("current") == 2 else None)

        self.tablas = {}
        definiciones = [
            ("tiempos", "Tiempos por volumen (mediana) · Ts y Tp en segundos",
             ["Registros", "Ts cálculo", "Tp A", "S A", "Ts completo", "Tp B", "S B",
              "E B", "Reg/s B", "Válido"]),
            ("escala", "Escalabilidad con 500 000 registros",
             ["Nodos", "T A (s)", "S A", "T B (s)", "S B", "S relativa B", "E relativa B"]),
            ("parciales", "Fase 3 · resultados parciales de cada nodo vs consolidación",
             ["Nodo", "Bytes", "Registros", "Ingresos (COP)", "Unidades", "Tiempo (s)"]),
            ("fallos", "Tolerancia a fallos (medido)",
             ["Escenario", "Consolidados", "Perdidos", "Error ingresos", "Reintentos",
              "Tiempo (s)", "Correcto"]),
            ("spark", "Apache Spark (modo local) vs Python secuencial",
             ["Registros", "Python (s)", "Python B (s)", "Spark (s)", "S Spark", "Idénticos"]),
            ("negocio", "Reporte de negocio (500 000 ventas)",
             ["Indicador", "Detalle", "Valor"]),
        ]
        for clave, titulo, columnas in definiciones:
            tarjeta = self._tarjeta(interior, fill="x", pady=(0, 10))
            tk.Label(tarjeta, text=titulo, bg=self.c["card"], fg=self.c["texto"],
                     font=(FUENTE, 10, "bold")).pack(anchor="w", padx=12, pady=(10, 4))
            tabla = ttk.Treeview(tarjeta, columns=columnas, show="headings", height=4)
            for col in columnas:
                tabla.heading(col, text=col)
                tabla.column(col, width=100, anchor="center")
            tabla.pack(fill="x", padx=12, pady=(0, 12))
            self.tablas[clave] = tabla

    def _llenar(self, clave, filas):
        tabla = self.tablas[clave]
        tabla.delete(*tabla.get_children())
        for fila in filas:
            tabla.insert("", "end", values=fila)
        tabla.configure(height=max(1, min(len(filas), 12)))

    def _refrescar_resultados(self):
        completo = leer_json("resultados_completos.json")
        if completo:
            self._llenar("tiempos", [(
                fmt(f["registros"], 0), fmt(f["t_secuencial_calculo_s"], 4),
                fmt(f["t_distribuido_A_s"], 4), f"{fmt(f['aceleracion_A'], 2)}×",
                fmt(f["t_secuencial_completo_s"], 4), fmt(f["t_distribuido_B_s"], 4),
                f"{fmt(f['aceleracion_B'], 2)}×", fmt(f["eficiencia_B"], 2),
                fmt(f["throughput_B_reg_s"], 0), "Sí" if f["resultados_iguales"] else "No")
                for f in completo["tabla_tiempos"]])
            self._llenar("escala", [(
                f["nodos"], fmt(f["t_A_s"], 4), f"{fmt(f['aceleracion_A'], 2)}×",
                fmt(f["t_B_s"], 4), f"{fmt(f['aceleracion_B'], 2)}×",
                f"{fmt(f['aceleracion_relativa_B'], 2)}×", fmt(f["eficiencia_relativa_B"], 2))
                for f in completo["escalabilidad"]])
        part = leer_json("particiones.json")
        if part:
            filas = [(n["nodo"], fmt(n["bytes"], 0), fmt(n["registros"], 0),
                      fmt(n["ingresos_COP"], 2), fmt(n["unidades"], 0), fmt(n["tiempo_s"], 4))
                     for n in part["nodos"]]
            s, f = part["suma_de_parciales"], part["consolidado_final"]
            filas.append(("Σ parciales", "", fmt(s["registros"], 0), fmt(s["ingresos_COP"], 2),
                          fmt(s["unidades"], 0), ""))
            filas.append(("Consolidado", "", fmt(f["registros"], 0), fmt(f["ingresos_COP"], 2),
                          fmt(f["unidades"], 0),
                          "✓ = secuencial" if part["coincide_con_secuencial"] else "✕"))
            self._llenar("parciales", filas)
        fallos = leer_json("tolerancia_fallos.json")
        if fallos:
            self._llenar("fallos", [(
                e["escenario"], f"{fmt(e['registros_consolidados'], 0)} / "
                                f"{fmt(e['registros_esperados'], 0)}",
                fmt(e["registros_perdidos"], 0), f"{fmt(e['error_ingresos_pct'], 2)} %",
                e["reintentos"], fmt(e["tiempo_total_s"], 3),
                "✓" if e["resultado_correcto"] else "✕") for e in fallos["escenarios"]])
        spark = leer_json("spark_resultados.json")
        if spark:
            self._llenar("spark", [(
                fmt(f["registros"], 0), fmt(f["t_python_secuencial_s"], 4),
                fmt(f.get("t_python_bloques_B_s", 0), 4), fmt(f["t_spark_s"], 4),
                f"{fmt(f['aceleracion_vs_python'], 2)}×",
                "Sí" if f["resultados_iguales"] else "No")
                for f in spark["tabla"]])
        else:
            self._llenar("spark", [("—", "PySpark no ejecutado", "pip install pyspark==3.5.3",
                                    "", "", "")])
        cons = leer_json("resultados_consolidados.json")
        if cons:
            filas = [("Ingresos totales", "COP", fmt(cons["ingresos_totales_COP"], 2)),
                     ("Unidades totales", "", fmt(cons["unidades_totales"], 0))]
            filas += [(f"Top {i}", p["producto"], f"{fmt(p['unidades'], 0)} u.")
                      for i, p in enumerate(cons["top_cinco_productos"], start=1)]
            ciudades = sorted(cons["ingresos_por_ciudad_COP"].items(), key=lambda x: -x[1])
            filas += [("Ciudad", k, fmt(v, 2)) for k, v in ciudades]
            meses = sorted(cons["ingresos_por_mes_COP"].items(), key=lambda x: -x[1])[:3]
            filas += [("Mes más activo", k, fmt(v, 2)) for k, v in meses]
            self._llenar("negocio", filas)

    # ------------------------------------------------------------------ Datos
    def _tab_datos(self):
        marco = ttk.Frame(self.cuaderno, padding=(2, 12, 2, 2))
        self.cuaderno.add(marco, text="Datos")
        barra = ttk.Frame(marco)
        barra.pack(fill="x", pady=(0, 8))
        ttk.Label(barra, text="Archivo").pack(side="left")
        self.combo_datos = ttk.Combobox(barra, state="readonly", width=18,
                                        values=["ventas_10k.csv", "ventas_100k.csv",
                                                "ventas_500k.csv"])
        self.combo_datos.set("ventas_10k.csv")
        self.combo_datos.pack(side="left", padx=(6, 14))
        self.combo_datos.bind("<<ComboboxSelected>>", lambda e: self._cargar_datos())
        ttk.Label(barra, text="Buscar").pack(side="left")
        self.filtro = tk.StringVar()
        entrada = ttk.Entry(barra, textvariable=self.filtro, width=24)
        entrada.pack(side="left", padx=6)
        entrada.bind("<Return>", lambda e: self._cargar_datos())
        ttk.Button(barra, text="Filtrar", command=self._cargar_datos).pack(side="left")
        self.info_datos = ttk.Label(barra, text="", style="Muted.TLabel")
        self.info_datos.pack(side="right")
        contenedor = ttk.Frame(marco)
        contenedor.pack(fill="both", expand=True)
        self.tabla_datos = ttk.Treeview(contenedor, show="headings")
        bv = ttk.Scrollbar(contenedor, orient="vertical", command=self.tabla_datos.yview)
        self.tabla_datos.configure(yscrollcommand=bv.set)
        bv.pack(side="right", fill="y")
        self.tabla_datos.pack(side="left", fill="both", expand=True)

    def _cargar_datos(self):
        ruta = os.path.join(DATOS, self.combo_datos.get())
        self.tabla_datos.delete(*self.tabla_datos.get_children())
        if not os.path.exists(ruta):
            self.info_datos.configure(text="Archivo no disponible: ejecute el paso 1")
            return
        filtro = self.filtro.get().strip().lower()
        total = coincidencias = 0
        with open(ruta, newline="", encoding="utf-8") as archivo:
            lector = csv.reader(archivo)
            cabecera = next(lector)
            self.tabla_datos["columns"] = cabecera
            for col in cabecera:
                self.tabla_datos.heading(col, text=col)
                self.tabla_datos.column(col, width=140, anchor="w")
            for fila in lector:
                total += 1
                if filtro and filtro not in ",".join(fila).lower():
                    continue
                coincidencias += 1
                if coincidencias <= 500:
                    self.tabla_datos.insert("", "end", values=fila)
        texto = f"{fmt(total, 0)} registros"
        if filtro:
            texto += f" · {fmt(coincidencias, 0)} coinciden con «{filtro}»"
        self.info_datos.configure(text=texto + " · se muestran hasta 500")

    # ---------------------------------------------------------------- Figuras
    def _tab_figuras(self):
        marco = ttk.Frame(self.cuaderno, padding=(2, 12, 2, 2))
        self.cuaderno.add(marco, text="Figuras")
        barra = ttk.Frame(marco)
        barra.pack(fill="x")
        self.combo_fig = ttk.Combobox(barra, state="readonly", width=40)
        self.combo_fig.pack(side="left")
        self.combo_fig.bind("<<ComboboxSelected>>",
                            lambda e: self._mostrar_figura(self.combo_fig.current()))
        ttk.Button(barra, text="◀", width=3, command=lambda: self._mostrar_figura(
            self.indice_figura - 1)).pack(side="left", padx=(8, 2))
        ttk.Button(barra, text="▶", width=3, command=lambda: self._mostrar_figura(
            self.indice_figura + 1)).pack(side="left")
        ttk.Button(barra, text="Abrir en tamaño completo", command=self._abrir_figura).pack(
            side="right")
        self.visor = tk.Label(marco, bg=self.c["card"], anchor="center")
        self.visor.pack(fill="both", expand=True, pady=(10, 0))
        self.visor.bind("<Configure>", lambda e: self._mostrar_figura(self.indice_figura, False))

    def _cargar_figuras(self):
        if os.path.isdir(DIAGRAMAS):
            self.figuras = sorted(f for f in os.listdir(DIAGRAMAS) if f.endswith(".png"))
        self.combo_fig["values"] = self.figuras
        if self.figuras:
            self._mostrar_figura(min(self.indice_figura, len(self.figuras) - 1))

    def _mostrar_figura(self, indice, actualizar_combo=True):
        if not self.figuras:
            return
        self.indice_figura = indice % len(self.figuras)
        if actualizar_combo:
            self.combo_fig.current(self.indice_figura)
        ruta = os.path.join(DIAGRAMAS, self.figuras[self.indice_figura])
        ancho = max(self.visor.winfo_width() - 10, 400)
        alto = max(self.visor.winfo_height() - 10, 300)
        try:
            try:  # Pillow da un escalado suave y exacto
                from PIL import Image, ImageTk
                original = Image.open(ruta)
                escala = min(ancho / original.width, alto / original.height, 1)
                imagen = ImageTk.PhotoImage(original.resize(
                    (int(original.width * escala), int(original.height * escala)),
                    Image.LANCZOS))
            except ImportError:  # solo biblioteca estándar
                imagen = tk.PhotoImage(file=ruta)
                factor = max(1, math.ceil(max(imagen.width() / ancho, imagen.height() / alto)))
                if factor > 1:
                    imagen = imagen.subsample(factor, factor)
            self.fotos = [imagen]
            self.visor.configure(image=imagen, text="")
        except Exception as error:
            self.visor.configure(image="", text=f"No se pudo mostrar: {error}")

    def _abrir_figura(self):
        if self.figuras:
            self._abrir(os.path.join(DIAGRAMAS, self.figuras[self.indice_figura]))

    # ---------------------------------------------------------------- Consola
    def _tab_consola(self):
        marco = ttk.Frame(self.cuaderno, padding=(2, 12, 2, 2))
        self.cuaderno.add(marco, text="Consola")
        c = self.c
        self.consola = tk.Text(marco, wrap="word", font=("Consolas", 9), bg=c["consola_bg"],
                               fg=c["consola_fg"], relief="flat", padx=12, pady=10,
                               insertbackground=c["consola_fg"])
        barra = ttk.Scrollbar(marco, orient="vertical", command=self.consola.yview)
        self.consola.configure(yscrollcommand=barra.set)
        barra.pack(side="right", fill="y")
        self.consola.pack(side="left", fill="both", expand=True)
        for etiqueta, color in (("ok", "#7FD1AE"), ("error", "#FF8A80"), ("info", "#8AB4F8"),
                                ("cmd", "#F6C177")):
            self.consola.tag_configure(etiqueta, foreground=color)
        for texto, etiqueta in self.texto_consola[-2000:]:
            self.consola.insert("end", texto + "\n", etiqueta or ())
        if not self.texto_consola:
            self._log("Interfaz lista. Configure los parámetros y pulse «Ejecutar todo» (F5).",
                      "info")

    # -------------------------------------------------------------- Ejecución
    def _entorno(self, extra=None):
        entorno = dict(os.environ)
        entorno.update({"PYTHONIOENCODING": "utf-8", "PYTHONUNBUFFERED": "1",
                        "LAB_NODOS": str(self.nodos.get()),
                        "LAB_DATASET": self.dataset.get(),
                        "LAB_REPETICIONES": str(self.repeticiones.get())})
        entorno.update(extra or {})
        return entorno

    def _ejecutar(self, archivos, extra_env=None, marcar=True):
        if self.activo:
            messagebox.showwarning("Proceso en curso", "Espere a que termine la tarea actual.")
            return
        self.activo = True
        self.btn_todo.state(["disabled"])
        self.btn_detener.state(["!disabled"])
        threading.Thread(target=self._secuencia, args=(archivos, extra_env, marcar),
                         daemon=True).start()

    def _ejecutar_todo(self):
        archivos = [p[0] for p in PASOS
                    if not (p[0] == "procesamiento_spark.py" and not HAY_SPARK)]
        self.cuaderno.select(5)
        self._ejecutar(archivos)

    def _detener(self):
        self.detener = True
        if self.proceso and self.proceso.poll() is None:
            self.proceso.terminate()
            self._log("Ejecución detenida por el usuario.", "error")

    def _secuencia(self, archivos, extra_env, marcar):
        self.detener = False
        inicio = time.perf_counter()
        total = len(archivos)
        self.cola.put(("progreso", 0, None))
        for indice, archivo in enumerate(archivos, start=1):
            if self.detener:
                break
            if marcar:
                self.cola.put(("paso", archivo, "ejecutando"))
            self.cola.put(("estado", f"Ejecutando {archivo} ({indice}/{total})…", None))
            self._log(f"\n$ python {archivo}", "cmd")
            banderas = getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0
            codigo = -1
            try:
                self.proceso = subprocess.Popen(
                    [sys.executable, os.path.join(BASE, archivo)], cwd=BASE,
                    env=self._entorno(extra_env), stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT, text=True, encoding="utf-8",
                    errors="replace", creationflags=banderas)
                for linea in self.proceso.stdout:
                    texto = linea.rstrip("\n")
                    if texto.startswith("@@EVENTO "):
                        try:
                            self.cola.put(("evento", json.loads(texto[9:]), None))
                        except ValueError:
                            pass
                        continue
                    if texto.strip():
                        self._log(texto, self._etiqueta(texto))
                codigo = self.proceso.wait()
            except Exception as error:
                self._log(f"Error al ejecutar {archivo}: {error}", "error")
            if codigo == 0:
                self._log(f"✔ {archivo} terminó correctamente", "ok")
            else:
                self._log(f"✘ {archivo} terminó con código {codigo}", "error")
            if marcar:
                self.cola.put(("paso", archivo, "ok" if codigo == 0 else "error"))
            self.cola.put(("progreso", indice / total * 100, None))
        duracion = time.perf_counter() - inicio
        self.cola.put(("estado", f"Finalizado en {duracion:.1f} s", None))
        self.cola.put(("fin", None, None))

    @staticmethod
    def _etiqueta(texto):
        if texto.startswith(("Generado", "Resultados guardados", "PDF generado",
                             "Documento generado", "Diagramas listos")) or "CORRECTO" in texto \
                and "INCORRECTO" not in texto:
            return "ok"
        if "Traceback" in texto or "Error" in texto or "INCORRECTO" in texto:
            return "error"
        if texto.startswith("==="):
            return "info"
        return None

    def _log(self, mensaje, etiqueta=None):
        self.cola.put(("log", mensaje, etiqueta))

    def _drenar_cola(self):
        try:
            while True:
                tipo, valor, extra = self.cola.get_nowait()
                if tipo == "log":
                    self.texto_consola.append((valor, extra))
                    if self.consola.winfo_exists():
                        self.consola.insert("end", valor + "\n", extra or ())
                        self.consola.see("end")
                elif tipo == "estado":
                    self.lbl_estado.configure(text=valor)
                elif tipo == "progreso":
                    self.progreso.configure(value=valor)
                elif tipo == "paso":
                    self.estado_pasos[valor] = extra
                    self._pintar_paso(valor)
                elif tipo == "evento":
                    self._evento(valor)
                elif tipo == "fin":
                    self.activo = False
                    self.btn_todo.state(["!disabled"])
                    self.btn_detener.state(["disabled"])
                    self._refrescar_todo()
        except queue.Empty:
            pass
        self.root.after(80, self._drenar_cola)

    def _limpiar_consola(self):
        self.texto_consola.clear()
        self.consola.delete("1.0", "end")

    # ----------------------------------------------------------------- otros
    def _refrescar_todo(self):
        self._refrescar_panel()
        self._refrescar_resultados()
        self._cargar_datos()
        self._cargar_figuras()

    def _alternar_tema(self):
        if self.activo:
            messagebox.showinfo("Tema", "Cambie el tema cuando termine la ejecución.")
            return
        pestana = self.cuaderno.index("current")
        self.tema.set("oscuro" if self.tema.get() == "claro" else "claro")
        self._construir()
        self.cuaderno.select(pestana)

    def _abrir_informe(self):
        for nombre in ("Informe_Laboratorio_Guia8.pdf", "Informe_Laboratorio_Guia8.docx"):
            ruta = os.path.join(BASE, nombre)
            if os.path.exists(ruta):
                return self._abrir(ruta)
        messagebox.showinfo("Informe", "Aún no se ha generado el informe.")

    def _abrir(self, ruta):
        if not os.path.exists(ruta):
            messagebox.showinfo("No disponible", f"No se encontró: {ruta}")
            return
        if os.name == "nt":
            os.startfile(ruta)
        else:
            subprocess.Popen(["xdg-open", ruta])

    def _salir(self):
        if self.activo and not messagebox.askokcancel(
                "Salir", "Hay una tarea en curso. ¿Desea salir de todos modos?"):
            return
        self._detener() if self.activo else None
        self.root.destroy()


def main():
    try:  # nitidez en pantallas de alta densidad (Windows)
        import ctypes
        ctypes.windll.shcore.SetProcessDpiAwareness(1)
    except Exception:
        pass
    root = tk.Tk()
    Aplicacion(root)
    root.mainloop()


if __name__ == "__main__":
    main()
