@echo off
cd /d "%~dp0"
echo Abriendo interfaz grafica de la Guia 8...
python interfaz_grafica.py
if errorlevel 1 pause
