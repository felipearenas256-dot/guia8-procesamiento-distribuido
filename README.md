# Guía 8 — Procesamiento Distribuido de Datos

Solución completa de la guía de laboratorio: datos sintéticos, arquitectura, código
comentado, pruebas de rendimiento reales, simulación de fallos, análisis y conclusiones.

## Interfaz gráfica (recomendado)

Doble clic en **`ABRIR_INTERFAZ.bat`** o `python interfaz_grafica.py`.

- **Barra lateral con el pipeline** (8 pasos) y el estado de cada uno; clic en un paso para ejecutarlo solo.
- **Panel** con indicadores y gráficas nativas interactivas (pase el mouse para ver los valores).
- **Simulador de fallos en vivo**: elija el escenario y el nodo que falla, y vea cómo cada nodo procesa, cae y se recupera.
- **Resultados** (estrategias A y B, escalabilidad, parciales por nodo, fallos, Spark, reporte de negocio),
  **Datos** con búsqueda, **Figuras** y **Consola**.
- Configuración de nodos (1–8), volumen de datos y repeticiones; tema claro u oscuro; `F5` ejecuta todo.

Solo usa la biblioteca estándar (`tkinter`); si Pillow está instalado, las figuras se escalan con más calidad.

## Línea de comandos

```powershell
python generar_datos.py              # Fase 1: datos/*.csv (semilla 42)
python procesamiento_distribuido.py  # Fases 3-4: estrategias A y B, escalabilidad 1-8 nodos
python experimento_intensivo.py      # Carga CPU-intensiva
python tolerancia_fallos.py          # Fase 5: caída, reintento y timeout de un nodo
python generar_datos.py --grande     # Opcional: 2 millones de registros (104 MB)
python procesamiento_spark.py        # Apache Spark 3.5.3 (requiere pyspark y Java)
python generar_diagramas.py          # Figuras PNG en diagramas/
python generar_documento.py          # Informe DOCX desde el Markdown
python exportar_pdf.py               # Informe PDF (requiere Microsoft Word)
```

Variables de entorno: `LAB_NODOS`, `LAB_DATASET` (todos | 10k | 100k | 500k), `LAB_REPETICIONES`,
`LAB_FALLO_NODO`, `LAB_ESCENARIO`.

## Resultados principales (4 nodos, mediana de 5 repeticiones)

| Registros | A · envío de registros | B · localidad de datos | Resultado |
|---:|---:|---:|:---:|
| 10 000 | 0,04× | 0,08× | idéntico |
| 100 000 | 0,16× | 1,22× | idéntico |
| 500 000 | 0,19× | **1,94×** | idéntico |

- Escalabilidad B (500 000): 1,59× con 2 nodos, 2,12× con 4 y 2,13× con 8 (6 núcleos físicos).
- Si un nodo cae sin tolerancia a fallos, se pierde el 25 % de los registros y los ingresos quedan
  24,8 % por debajo **sin aviso**; con detección + reintento el resultado vuelve a ser exacto.

- Apache Spark 3.5.3 (local[4]) da resultados idénticos; con 2 millones de registros iguala al
  Python secuencial (1,02×), mientras que la estrategia B en Python es 2,75× más rápida.

> Los "nodos" son procesos locales (`multiprocessing`): es una simulación, no un clúster real.

## Contenido

| Archivo / carpeta | Descripción |
|---|---|
| `Informe_Laboratorio_Guia8.docx` / `.pdf` | **Documento para entregar** (30 páginas aprox.) |
| `Informe_Laboratorio_Guia8.md` | Fuente del informe |
| `interfaz_grafica.py` | Interfaz gráfica v3 |
| `procesamiento_distribuido.py` | Particionamiento A y B, nodos, consolidación, verificación, mediciones |
| `tolerancia_fallos.py` | Inyección de fallos, checkpoints, reintento y timeout |
| `procesamiento_spark.py` | Mismas consultas con PySpark, comparadas con Python |
| `datos/` | Los tres CSV (el de 2 millones no va en el ZIP: se crea con `--grande`) |
| `resultados/` | Tablas CSV/JSON de cada prueba y checkpoints |
| `diagramas/` | 8 figuras del informe |
| `evidencias/` | Capturas de la interfaz gráfica |

**Pendiente de completar en el informe:** asignatura, docente, grupo, integrantes, roles y enlace de Drive/OneDrive.

### Instalar PySpark si la descarga desde PyPI es lenta

PyPI entregaba el paquete a ~45 KB/s (unas 2 h). Se instaló desde conda-forge, que trae el mismo
PySpark 3.5.3 y descarga en segundos: se baja `pyspark-3.5.3-pyhd8ed1ab_1.conda` de
`conda.anaconda.org/conda-forge/noarch/`, se extrae su carpeta `site-packages` en la del Python
del equipo y se instala `pip install py4j==0.10.9.7`.
