@echo off
echo ==================================================
echo Instalando Bibliotecas do Mosaico EXPO...
echo ==================================================

:: 1. Verifica se a .venv existe, se nao cria
if not exist ".venv\Scripts\python.exe" (
    echo Criando ambiente virtual (.venv)...
    python -m venv .venv
    if errorlevel 1 (
        echo [ERRO] Falha ao criar ambiente virtual!
        echo Verifique se o Python esta instalado no Windows.
        pause
        exit /b
    )
)

echo.
echo Instalando pacotes do requirements.txt dentro da .venv...
echo (Isso pode levar de 1 a 2 minutos, aguarde...)
echo.

.venv\Scripts\python.exe -m pip install --upgrade pip
.venv\Scripts\python.exe -m pip install -r requirements.txt

if errorlevel 1 (
    echo.
    echo [ERRO] Falha ao instalar alguma biblioteca.
    echo Verifique sua conexao com a internet e tente novamente.
) else (
    echo.
    echo ==================================================
    echo SUCESSO! Todas as bibliotecas foram instaladas.
    echo Agora execute o test_diagnostico.bat para validar!
    echo ==================================================
)

echo.
pause
