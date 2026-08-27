@echo off
echo ==================================================
echo Encerrando servicos Mosaico EXPO no Windows...
echo ==================================================

:: Encerra as janelas do terminal com base nos titulos definidos no start
taskkill /FI "WINDOWTITLE eq HTTP Server*" /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq Auto Mosaic Watcher*" /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq Indexador*" /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq Watch Manifest*" /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq Captura Camera*" /F >nul 2>&1

:: Alternativa: encerrar os processos executaveis caso tenham ficado orfaos
taskkill /IM python.exe /F >nul 2>&1
taskkill /IM node.exe /F >nul 2>&1

:: Encerra todos os terminais
taskkill /IM cmd.exe /F >nul 2>&1
taskkill /IM WindowsTerminal.exe /F >nul 2>&1

:: Encerra os navegadores
taskkill /IM chrome.exe /F >nul 2>&1
taskkill /IM msedge.exe /F >nul 2>&1
taskkill /IM firefox.exe /F >nul 2>&1

echo ==================================================
echo Todos os servicos Mosaico EXPO foram encerrados!
echo ==================================================
pause
