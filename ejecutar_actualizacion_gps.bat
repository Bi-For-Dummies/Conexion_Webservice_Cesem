@echo off
echo ============================================ >> "C:\Users\Coordinador_SST\Python\Proyecto CESEM\task_log.txt"
echo Inicio: %date% %time% >> "C:\Users\Coordinador_SST\Python\Proyecto CESEM\task_log.txt"
echo ============================================ >> "C:\Users\Coordinador_SST\Python\Proyecto CESEM\task_log.txt"

cd /d "C:\Users\Coordinador_SST\Python\Proyecto CESEM"

echo Ejecutando Python... >> "C:\Users\Coordinador_SST\Python\Proyecto CESEM\task_log.txt"
"C:\Users\Coordinador_SST\AppData\Local\Programs\Python\Python313\python.exe" extraer_datos.py >> "C:\Users\Coordinador_SST\Python\Proyecto CESEM\task_log.txt" 2>&1

echo Codigo de salida: %errorlevel% >> "C:\Users\Coordinador_SST\Python\Proyecto CESEM\task_log.txt"
echo Fin: %date% %time% >> "C:\Users\Coordinador_SST\Python\Proyecto CESEM\task_log.txt"
echo. >> "C:\Users\Coordinador_SST\Python\Proyecto CESEM\task_log.txt"

exit