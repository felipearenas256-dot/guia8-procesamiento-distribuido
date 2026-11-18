"""Exporta el informe .docx a PDF usando Microsoft Word.

Uso:
    python exportar_pdf.py

Requiere: pywin32 (pip install pywin32) y Microsoft Word instalado.
"""

import os
import pythoncom
import win32com.client

BASE = os.path.dirname(os.path.abspath(__file__))
DOCX = os.path.join(BASE, "Informe_Laboratorio_Guia8.docx")
PDF = os.path.join(BASE, "Informe_Laboratorio_Guia8.pdf")


def exportar():
    pythoncom.CoInitialize()
    word = None
    documento = None
    try:
        word = win32com.client.Dispatch("Word.Application")
        word.Visible = False
        word.DisplayAlerts = 0
        documento = word.Documents.Open(os.path.abspath(DOCX))
        try:
            documento.Fields.Update()
            for indice in range(1, documento.TablesOfContents.Count + 1):
                documento.TablesOfContents(indice).Update()
        except Exception as aviso:
            print("Aviso al actualizar el índice:", aviso)
        documento.Save()  # guarda el DOCX con el índice ya poblado
        documento.SaveAs(os.path.abspath(PDF), FileFormat=17)  # 17 = PDF
        print("PDF generado:", PDF)
    finally:
        if documento is not None:
            documento.Close(False)
        if word is not None:
            word.Quit()
        pythoncom.CoUninitialize()


if __name__ == "__main__":
    exportar()
