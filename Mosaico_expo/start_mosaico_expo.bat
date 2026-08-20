@echo off
echo ==================================================
echo Iniciando servicos Mosaico EXPO no Windows...
echo ==================================================

:: Verifica se a pasta .venv existe
if not exist ".venv" (
    echo Erro: Ambiente virtual (.venv) nao encontrado!
    echo Por favor, crie o .venv e instale os requirements antes de iniciar.
    pause
    exit /b
)

:: 1) Servidor HTTP estatico (Mural) na porta 8000
echo - Iniciando servidor HTTP (Porta 8000)...
start "HTTP Server" cmd /k ".venv\Scripts\activate && python -u serve_site.py"

:: 2) Watcher de mosaicos (auto_mosaic)
echo - Iniciando auto_mosaic watcher...
start "Auto Mosaic Watcher" cmd /k ".venv\Scripts\activate && python -u auto_mosaic.py"

:: 3) Indexador de tiles
echo - Iniciando indexador...
start "Indexador" cmd /k ".venv\Scripts\activate && python -u indexador.py"

:: 4) Watcher de manifest (watch-manifest.js) na porta 8081
echo - Iniciando watch-manifest...
start "Watch Manifest" cmd /k "node watch-manifest.js"

:: 5) Captura da camera (webcam)
echo - Iniciando captura da webcam...
start "Captura Camera" cmd /k ".venv\Scripts\activate && python -u camera/captura.py"

echo ==================================================
echo Todos os servicos foram iniciados em janelas separadas.
echo Mantenha as janelas abertas enquanto o sistema estiver ativo.
echo ==================================================
pause
