@echo off
cd /d "%~dp0"
echo ==================================================
echo TESTADOR VISUAL DE CAMERAS - MOSAICO EXPO
echo ==================================================
echo Encerrando servicos anteriores para liberar as cameras...
taskkill /F /IM python.exe /T >nul 2>&1
timeout /t 2 /nobreak >nul 2>&1

echo Tirando fotos de teste no Indice 0 e Indice 1...
.venv\Scripts\activate && python testar_cameras.py
echo.
pause
