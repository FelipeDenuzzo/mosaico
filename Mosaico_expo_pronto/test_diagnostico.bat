@echo off
echo ==================================================
echo Executando Diagnostico Mosaico EXPO...
echo ==================================================

if exist ".venv\Scripts\python.exe" (
    echo [OK] Usando Python do ambiente virtual (.venv)...
    .venv\Scripts\python.exe gerar_relatorio_diagnostico.py
) else (
    echo [AVISO] Ambiente .venv nao encontrado! Testando com Python do sistema...
    python gerar_relatorio_diagnostico.py
)

echo.
echo ==================================================
echo Pressione qualquer tecla para fechar esta janela...
echo ==================================================
pause >nul
