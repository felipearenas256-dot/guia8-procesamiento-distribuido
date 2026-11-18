"""Convierte Informe_Laboratorio_Guia8.md en un documento Word (.docx) listo para entregar.

Uso:
    python generar_documento.py

Requiere: python-docx (pip install python-docx)
"""

import os
import re

from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Inches, Pt, RGBColor

BASE = os.path.dirname(os.path.abspath(__file__))
ORIGEN = os.path.join(BASE, "Informe_Laboratorio_Guia8.md")
DESTINO = os.path.join(BASE, "Informe_Laboratorio_Guia8.docx")

AZUL = RGBColor(0x1F, 0x3B, 0x63)
GRIS = RGBColor(0x44, 0x44, 0x44)


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------

def limpiar(texto):
    """Limpia marcadores de LaTeX y normaliza espacios."""
    texto = texto.replace(r"\[", "").replace(r"\]", "")
    texto = texto.replace(r"\(", "").replace(r"\)", "")
    texto = re.sub(r"\\text\{([^}]*)\}", r"\1", texto)
    texto = re.sub(r"\\frac\{([^}]*)\}\{([^}]*)\}", r"\1/\2", texto)
    texto = texto.replace(r"\qquad", "    ")
    return texto


def anadir_inline(parrafo, texto, tamano=None):
    """Añade texto con negrita, cursiva y código en línea."""
    texto = limpiar(texto)
    patron = re.compile(r"(\*\*[^*]+\*\*|`[^`]+`|\*[^*]+\*|\[[^\]]+\]\([^)]+\))")
    for parte in patron.split(texto):
        if not parte:
            continue
        run = parrafo.add_run()
        if tamano:
            run.font.size = Pt(tamano)
        if parte.startswith("**") and parte.endswith("**"):
            run.text = parte[2:-2]
            run.bold = True
        elif parte.startswith("`") and parte.endswith("`"):
            run.text = parte[1:-1]
            run.font.name = "Consolas"
            run.font.color.rgb = GRIS
        elif parte.startswith("*") and parte.endswith("*"):
            run.text = parte[1:-1]
            run.italic = True
        elif parte.startswith("[") and "](" in parte:
            run.text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", parte)
            run.font.color.rgb = AZUL
            run.underline = True
        else:
            run.text = parte


def sombrear(celda, color="E8EDF4"):
    tc_pr = celda._tc.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), color)
    tc_pr.append(shd)


def anadir_imagen(documento, ruta, texto):
    """Incrusta una figura y su pie de figura centrado."""
    ruta = ruta if os.path.isabs(ruta) else os.path.join(BASE, ruta)
    if not os.path.exists(ruta):
        print("Imagen no encontrada:", ruta)
        return
    parrafo = documento.add_paragraph()
    parrafo.alignment = WD_ALIGN_PARAGRAPH.CENTER
    parrafo.paragraph_format.space_before = Pt(6)
    parrafo.paragraph_format.space_after = Pt(2)
    parrafo.add_run().add_picture(ruta, width=Inches(6.2))

    pie = documento.add_paragraph()
    pie.alignment = WD_ALIGN_PARAGRAPH.CENTER
    pie.paragraph_format.space_after = Pt(10)
    run = pie.add_run(limpiar(texto))
    run.italic = True
    run.font.size = Pt(8.5)
    run.font.color.rgb = GRIS


def anadir_toc(documento):
    """Inserta el campo de índice (Word lo actualiza al exportar)."""
    titulo = documento.add_paragraph()
    titulo.style = documento.styles["Heading 1"]
    titulo.add_run("Índice")
    parrafo = documento.add_paragraph()
    campo = OxmlElement("w:fldSimple")
    campo.set(qn("w:instr"), r'TOC \o "1-2" \h \z \u')
    parrafo._p.append(campo)
    nota = documento.add_paragraph()
    nota.add_run(
        "(Si el índice no se muestra, seleccione todo el documento y pulse F9 en Word.)"
    ).italic = True
    nota.runs[0].font.size = Pt(8)
    nota.runs[0].font.color.rgb = GRIS
    documento.add_page_break()


def anadir_tabla(documento, filas):
    if not filas:
        return
    columnas = max(len(f) for f in filas)
    tabla = documento.add_table(rows=0, cols=columnas)
    tabla.style = "Table Grid"
    tabla.alignment = WD_TABLE_ALIGNMENT.CENTER
    tabla.autofit = True

    for indice, fila in enumerate(filas):
        celdas = tabla.add_row().cells
        for i in range(columnas):
            valor = fila[i] if i < len(fila) else ""
            celda = celdas[i]
            celda.text = ""
            parrafo = celda.paragraphs[0]
            parrafo.paragraph_format.space_after = Pt(2)
            anadir_inline(parrafo, valor, tamano=8.5)
            if indice == 0:
                sombrear(celda)
                for run in parrafo.runs:
                    run.bold = True

    if tabla.rows:
        tr_pr = tabla.rows[0]._tr.get_or_add_trPr()
        cabecera = OxmlElement("w:tblHeader")
        cabecera.set(qn("w:val"), "true")
        tr_pr.append(cabecera)
    for fila in tabla.rows:
        tr_pr = fila._tr.get_or_add_trPr()
        tr_pr.append(OxmlElement("w:cantSplit"))
    documento.add_paragraph()


def anadir_codigo(documento, lineas, tamano=8.5):
    parrafo = documento.add_paragraph()
    parrafo.paragraph_format.left_indent = Inches(0.25)
    parrafo.paragraph_format.space_after = Pt(6)
    p_pr = parrafo._p.get_or_add_pPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:fill"), "F2F2F2")
    p_pr.append(shd)
    for i, linea in enumerate(lineas):
        if i:
            parrafo.add_run().add_break()
        run = parrafo.add_run(linea)
        run.font.name = "Consolas"
        run.font.size = Pt(tamano)
        run.font.color.rgb = GRIS


def anadir_pie(documento):
    seccion = documento.sections[0]
    seccion.top_margin = Inches(0.85)
    seccion.bottom_margin = Inches(0.8)

    cabecera = seccion.header.paragraphs[0]
    cabecera.alignment = WD_ALIGN_PARAGRAPH.RIGHT
    run = cabecera.add_run("Guía 8 · Procesamiento Distribuido de Datos")
    run.font.size = Pt(8)
    run.font.color.rgb = GRIS

    pie = seccion.footer.paragraphs[0]
    pie.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = pie.add_run("Guía 8 - Procesamiento Distribuido de Datos   |   Página ")
    run.font.size = Pt(8)
    run.font.color.rgb = GRIS
    campo = OxmlElement("w:fldSimple")
    campo.set(qn("w:instr"), "PAGE")
    pie._p.append(campo)


def configurar_estilos(documento):
    normal = documento.styles["Normal"]
    normal.font.name = "Calibri"
    normal.font.size = Pt(10.5)
    normal.paragraph_format.space_after = Pt(6)

    titulo = documento.styles["Title"]
    titulo.font.name = "Calibri"
    titulo.font.size = Pt(26)
    titulo.font.bold = True
    titulo.font.color.rgb = AZUL

    for nombre, tamano in (("Heading 1", 15), ("Heading 2", 12.5), ("Heading 3", 11)):
        estilo = documento.styles[nombre]
        estilo.font.name = "Calibri"
        estilo.font.size = Pt(tamano)
        estilo.font.bold = True
        estilo.font.color.rgb = AZUL
        estilo.paragraph_format.space_before = Pt(14)
        estilo.paragraph_format.space_after = Pt(6)
        estilo.paragraph_format.keep_with_next = True


# ---------------------------------------------------------------------------
# Conversión
# ---------------------------------------------------------------------------

def guardar_con_respaldo(documento, destino):
    """Guarda de forma atómica; si Word tiene el archivo abierto, usa otro nombre."""
    temporal = destino + ".tmp"
    documento.save(temporal)
    try:
        os.replace(temporal, destino)
    except PermissionError:
        alternativo = destino.replace(".docx", "_actualizado.docx")
        os.replace(temporal, alternativo)
        print("El DOCX original estaba abierto; se guardó como:", alternativo)


def convertir():
    with open(ORIGEN, "r", encoding="utf-8") as archivo:
        lineas = archivo.read().splitlines()

    documento = Document()
    configurar_estilos(documento)
    anadir_pie(documento)

    i = 0
    primera_tabla = True
    while i < len(lineas):
        linea = lineas[i]
        texto = linea.strip()

        if not texto or (texto.startswith("<!--") and texto.endswith("-->")):
            i += 1
            continue

        # Bloque de código
        if texto.startswith("```"):
            i += 1
            bloque = []
            while i < len(lineas) and not lineas[i].strip().startswith("```"):
                bloque.append(lineas[i])
                i += 1
            i += 1
            anadir_codigo(documento, bloque)
            continue

        # Tablas
        if texto.startswith("|") and i + 1 < len(lineas) and re.match(
            r"^\s*\|[\s:\-|]+\|\s*$", lineas[i + 1]
        ):
            filas = []
            while i < len(lineas) and lineas[i].strip().startswith("|"):
                actual = lineas[i].strip()
                if not re.match(r"^\|[\s:\-|]+\|$", actual):
                    celdas = [c.strip() for c in actual.strip("|").split("|")]
                    filas.append(celdas)
                i += 1
            anadir_tabla(documento, filas)
            if primera_tabla:
                primera_tabla = False
                anadir_toc(documento)
            continue

        # Figuras
        m = re.match(r"^!\[([^\]]*)\]\(([^)]+)\)$", texto)
        if m:
            anadir_imagen(documento, m.group(2), m.group(1))
            i += 1
            continue

        # Encabezados
        m = re.match(r"^(#{1,3})\s+(.*)$", texto)
        if m:
            nivel = len(m.group(1))
            estilo = "Title" if nivel == 1 else f"Heading {nivel - 1}"
            if nivel == 2 and any(
                clave in m.group(2)
                for clave in ("1. Introducción", "5. Fase 1", "8. Fase 4", "9. Fase 5",
                              "13. Referencias", "Anexo A")
            ):
                documento.add_page_break()
            parrafo = documento.add_paragraph(style=estilo)
            anadir_inline(parrafo, m.group(2))
            i += 1
            continue

        # Regla horizontal
        if re.match(r"^-{3,}$", texto):
            i += 1
            continue

        # Cita / nota
        if texto.startswith(">"):
            bloque = []
            while i < len(lineas) and lineas[i].strip().startswith(">"):
                bloque.append(lineas[i].strip().lstrip(">").strip())
                i += 1
            parrafo = documento.add_paragraph()
            parrafo.paragraph_format.left_indent = Inches(0.3)
            anadir_inline(parrafo, " ".join(bloque))
            for run in parrafo.runs:
                run.italic = True
                run.font.color.rgb = GRIS
            continue

        # Listas
        if re.match(r"^-\s+", texto):
            parrafo = documento.add_paragraph(style="List Bullet")
            anadir_inline(parrafo, re.sub(r"^-\s+", "", texto))
            i += 1
            continue
        m_num = re.match(r"^(\d+)\.\s+(.*)$", texto)
        if m_num:
            # Se conserva el número del texto: "List Number" de Word continúa la
            # numeración entre listas distintas y desordena las secciones.
            parrafo = documento.add_paragraph()
            formato = parrafo.paragraph_format
            formato.left_indent = Inches(0.35)
            formato.first_line_indent = Inches(-0.25)
            formato.space_after = Pt(3)
            parrafo.add_run(f"{m_num.group(1)}.	")
            formato.tab_stops.add_tab_stop(Inches(0.35))
            anadir_inline(parrafo, m_num.group(2))
            i += 1
            continue

        # Párrafo normal (se agrupan las líneas consecutivas)
        bloque = [texto]
        i += 1
        while i < len(lineas):
            siguiente = lineas[i].strip()
            if (not siguiente or siguiente.startswith(("|", "```", "#", ">", "-"))
                    or re.match(r"^\d+\.\s+", siguiente)
                    or re.match(r"^-{3,}$", siguiente)):
                break
            bloque.append(siguiente)
            i += 1
        parrafo = documento.add_paragraph()
        anadir_inline(parrafo, " ".join(bloque))

    guardar_con_respaldo(documento, DESTINO)
    print("Documento generado:", DESTINO)
    print("Párrafos:", len(documento.paragraphs), "| Tablas:", len(documento.tables))


if __name__ == "__main__":
    convertir()
