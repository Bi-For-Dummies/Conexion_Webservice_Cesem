@echo off
echo ============================================ >> "C:\Users\Coordinador_SST\Python\Proyecto CESEM\task_log_V.txt"
echo Inicio: %date% %time% >> "C:\Users\Coordinador_SST\Python\Proyecto CESEM\task_log.txt"
echo ============================================ >> "C:\Users\Coordinador_SST\Python\Proyecto CESEM\task_log_V.txt"

cd /d "C:\Users\Coordinador_SST\Python\Proyecto CESEM"

echo Ejecutando Python... >> "C:\Users\Coordinador_SST\Python\Proyecto CESEM\task_log_V.txt"
"C:\Users\Coordinador_SST\AppData\Local\Programs\Python\Python313\python.exe" Reporte_Visitas.py >> "C:\Users\Coordinador_SST\Python\Proyecto CESEM\task_log_V.txt" 2>&1

echo Codigo de salida: %errorlevel% >> "C:\Users\Coordinador_SST\Python\Proyecto CESEM\task_log_V.txt"
echo Fin: %date% %time% >> "C:\Users\Coordinador_SST\Python\Proyecto CESEM\task_log_V.txt"
echo. >> "C:\Users\Coordinador_SST\Python\Proyecto CESEM\task_log_V.txt"

exit