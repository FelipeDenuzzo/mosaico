@echo off
cd /d "%~dp0"
echo ==================================================
echo Encerrando servicos Mosaico EXPO no Windows...
echo ==================================================

:: 1) Encerra processos Python em arvore (/T) de forma forcada (/F)
echo - Encerrando processos Python...
taskkill /F /IM python.exe /T >nul 2>&1

:: 2) Encerra processos Node.js
echo - Encerrando processos Node.js...
taskkill /F /IM node.exe /T >nul 2>&1

:: 3) Encerra as janelas dos servicos por titulo
echo - Fechando janelas de servico...
taskkill /FI "WINDOWTITLE eq HTTP Server*" /F /T >nul 2>&1
taskkill /FI "WINDOWTITLE eq Auto Mosaic Watcher*" /F /T >nul 2>&1
taskkill /FI "WINDOWTITLE eq Indexador*" /F /T >nul 2>&1
taskkill /FI "WINDOWTITLE eq Watch Manifest*" /F /T >nul 2>&1
taskkill /FI "WINDOWTITLE eq Captura Camera*" /F /T >nul 2>&1

:: 4) Encerra o navegador (Edge)
echo - Encerrando navegador Edge...
taskkill /F /IM msedge.exe /T >nul 2>&1

echo ==================================================
echo Todos os servicos Mosaico EXPO foram encerrados!
echo ==================================================
ping -n 3 127.0.0.1 >nul
exit
