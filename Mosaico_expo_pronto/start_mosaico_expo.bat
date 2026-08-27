@echo off
echo ==================================================
echo Iniciando servicos Mosaico EXPO no Windows...
echo ==================================================

:: Verifica se a pasta .venv existe
if exist ".venv" goto venv_ok
echo Erro: Ambiente virtual (.venv) nao encontrado!
echo Por favor, crie o .venv e instale os requirements antes de iniciar.
pause
exit /b

:venv_ok

:: 1) Servidor HTTP estatico (Mural) na porta 8000
echo - Iniciando servidor HTTP (Porta 8000)...
start /min "HTTP Server" cmd /k ".venv\Scripts\activate && python -u serve_site.py"

:: 2) Watcher de mosaicos (auto_mosaic)
echo - Iniciando auto_mosaic watcher...
start /min "Auto Mosaic Watcher" cmd /k ".venv\Scripts\activate && python -u auto_mosaic.py"

:: 3) Indexador de tiles
echo - Iniciando indexador...
start /min "Indexador" cmd /k ".venv\Scripts\activate && python -u indexador.py"

:: 4) Watcher de manifest (watch-manifest.js) na porta 8081
echo - Iniciando watch-manifest...
start /min "Watch Manifest" cmd /k "node watch-manifest.js"

:: 5) Captura da camera (webcam)
echo - Iniciando captura da webcam...
start /min "Captura Camera" cmd /k ".venv\Scripts\activate && python -u camera/captura.py"

:: 6) Abrir o navegador em modo Kiosk (Tela Cheia) após 5 segundos
echo - Iniciando navegador em Tela Cheia (Edge)...
ping -n 6 127.0.0.1 >nul
start msedge --kiosk "http://localhost:8000/mosaico-exibicao.html" --edge-kiosk-type=fullscreen

echo ==================================================
echo Todos os servicos foram iniciados em modo minimizado.
echo Mantenha as janelas minimizadas enquanto o sistema estiver ativo.
echo Esta janela sera fechada automaticamente...
echo ==================================================
ping -n 4 127.0.0.1 >nul
exit
