@echo off
setlocal
cd /d "%~dp0"

set "INPUT=data\ventes.csv"
set "OUTPUT=output"
if not "%~1"=="" set "INPUT=%~1"
if not "%~2"=="" set "OUTPUT=%~2"

call sbt clean package
if errorlevel 1 exit /b %errorlevel%

spark-submit --master "local[*]" --class fr.master.sparkventes.SalesAnalytics target\scala-2.13\spark-scala-ventes_2.13-1.0.0.jar "%INPUT%" "%OUTPUT%"
if errorlevel 1 exit /b %errorlevel%

endlocal
