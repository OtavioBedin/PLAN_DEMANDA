@echo off
chcp 65001 > nul
mode con: cols=140 lines=40

set "PASTA_RAIZ=%~dp0"
set "PASTA_SCRIPTS=%PASTA_RAIZ%00_SCRIPTS"
set "PYTHON_EXE=%PASTA_RAIZ%.venv\Scripts\python.exe"
set "ARQUIVO_PY=1_KRONA_ORGANIZAR_HIST_VEND_ESTATISTICO.py"

echo.
echo ============================================================
echo EXECUCAO DO HISTORICO ESTATISTICO
echo ============================================================
echo.

echo Pasta raiz:
echo %PASTA_RAIZ%
echo.

echo Pasta dos scripts:
echo %PASTA_SCRIPTS%
echo.

echo Python:
echo %PYTHON_EXE%
echo.

echo Arquivo Python:
echo %PASTA_SCRIPTS%\%ARQUIVO_PY%
echo.

if not exist "%PYTHON_EXE%" (
    echo ============================================================
    echo ERRO: PYTHON NAO ENCONTRADO
    echo ============================================================
    echo.
    echo Caminho procurado:
    echo %PYTHON_EXE%
    echo.
    pause
    exit /b 1
)

if not exist "%PASTA_SCRIPTS%\%ARQUIVO_PY%" (
    echo ============================================================
    echo ERRO: ARQUIVO PYTHON NAO ENCONTRADO
    echo ============================================================
    echo.
    echo Caminho procurado:
    echo %PASTA_SCRIPTS%\%ARQUIVO_PY%
    echo.
    pause
    exit /b 1
)

echo ============================================================
echo INICIANDO ROTINA PYTHON
echo ============================================================
echo.

"%PYTHON_EXE%" "%PASTA_SCRIPTS%\%ARQUIVO_PY%"

set "CODIGO_ERRO=%ERRORLEVEL%"

echo.

if not "%CODIGO_ERRO%"=="0" (
    echo ============================================================
    echo ERRO NA EXECUCAO DA ROTINA
    echo ============================================================
    echo.
    echo Codigo do erro: %CODIGO_ERRO%
    echo.
    pause
    exit /b %CODIGO_ERRO%
)

echo ============================================================
echo ROTINA FINALIZADA COM SUCESSO
echo ============================================================
echo.

pause
exit /b 0