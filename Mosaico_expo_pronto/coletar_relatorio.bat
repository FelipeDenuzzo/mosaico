@echo off
cd /d "%~dp0"
echo ==================================================
echo Gerando Relatorio Unificado de Diagnostico...
echo ==================================================

if exist ".venv\Scripts\activate.bat" (
    call .venv\Scripts\activate.bat
)

python gerar_relatorio_diagnostico.py

echo.
echo ==================================================
echo Relatorio gerado com sucesso em:
echo %~dp0RELATORIO_DIAGNOSTICO.txt
echo ==================================================
echo.
echo Abrindo relatorio no Bloco de Notas...
start notepad.exe "%~dp0RELATORIO_DIAGNOSTICO.txt"

pause
