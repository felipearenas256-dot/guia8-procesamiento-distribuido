"""Guía 8 - Fase 1: Preparación de los datos.

Genera conjuntos de datos SINTÉTICOS de ventas (sin información personal sensible)
para las pruebas de procesamiento distribuido.

Uso:
    python generar_datos.py

Salida:
    datos/ventas_10k.csv
    datos/ventas_100k.csv
    datos/ventas_500k.csv
    datos/ventas_2m.csv   (solo con --grande)
"""

import csv
import os
import random
from calendar import monthrange

BASE = os.path.dirname(os.path.abspath(__file__))
CARPETA_DATOS = os.path.join(BASE, "datos")
SEMILLA = 42
ANIO = 2025

CIUDADES = [
    "Bogotá", "Medellín", "Cali", "Barranquilla",
    "Bucaramanga", "Cartagena", "Cúcuta", "Pereira",
]

# (producto, precio base en COP)
PRODUCTOS = [
    ("Laptop", 2_500_000.0),
    ("Mouse", 45_000.0),
    ("Teclado", 89_000.0),
    ("Monitor", 950_000.0),
    ("Impresora", 720_000.0),
    ("Tablet", 1_350_000.0),
    ("Smartphone", 2_100_000.0),
    ("Audífonos", 160_000.0),
    ("DiscoDuro", 320_000.0),
    ("SillaOficina", 540_000.0),
]

# Los artículos de bajo costo se venden más por unidad (perfil de "top 5").
PESOS_PRODUCTO = [3, 10, 8, 4, 3, 4, 5, 9, 6, 4]

# Estacionalidad: noviembre y diciembre concentran mayor actividad comercial.
PESOS_MES = [0.8, 0.7, 0.9, 0.9, 1.0, 1.0, 0.9, 1.0, 1.1, 1.2, 1.6, 1.8]

# Distribución de unidades por transacción.
CANTIDADES = [1, 2, 3, 4, 5, 6, 8, 10]
PESOS_CANTIDAD = [25, 22, 18, 12, 9, 6, 5, 3]

CAMPOS = ["id_venta", "fecha", "ciudad", "producto", "cantidad", "precio_unitario"]


def generar_ventas(cantidad, ruta, semilla=SEMILLA):
    """Escribe `cantidad` registros de venta en `ruta` en formato CSV."""
    random.seed(semilla)
    os.makedirs(os.path.dirname(ruta), exist_ok=True)

    nombres = [p[0] for p in PRODUCTOS]
    with open(ruta, "w", newline="", encoding="utf-8") as archivo:
        escritor = csv.writer(archivo)
        escritor.writerow(CAMPOS)
        for i in range(1, cantidad + 1):
            mes = random.choices(range(1, 13), weights=PESOS_MES, k=1)[0]
            dia = random.randint(1, monthrange(ANIO, mes)[1])
            producto, precio_base = random.choices(
                PRODUCTOS, weights=PESOS_PRODUCTO, k=1
            )[0]

            cantidad_unidades = random.choices(CANTIDADES, weights=PESOS_CANTIDAD, k=1)[0]
            precio = round(precio_base * random.uniform(0.9, 1.1), 2)

            escritor.writerow([
                f"V-{i:07d}",
                f"{ANIO:04d}-{mes:02d}-{dia:02d}",
                random.choice(CIUDADES),
                producto,
                cantidad_unidades,
                f"{precio:.2f}",
            ])

    print(f"Generado: {ruta} ({cantidad:,} registros)")


def main():
    import sys
    conjuntos = [
        (10_000, "ventas_10k.csv"),
        (100_000, "ventas_100k.csv"),
        (500_000, "ventas_500k.csv"),
    ]
    # Conjunto opcional de 2 millones (104 MB) para la comparación con Spark:
    #   python generar_datos.py --grande
    if "--grande" in sys.argv:
        conjuntos = [(2_000_000, "ventas_2m.csv")]
    for cantidad, nombre in conjuntos:
        generar_ventas(cantidad, os.path.join(CARPETA_DATOS, nombre))


if __name__ == "__main__":
    main()
