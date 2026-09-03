@echo off
chcp 65001 > nul
mode con: cols=140 lines=40

for %%I in ("%~dp0..\..") do set "PASTA_RAIZ=%%~fI\"
set "PASTA_SCRIPTS=%PASTA_RAIZ%00_SCRIPTS"
set "PYTHON_EXE=%PASTA_RAIZ%.venv\Scripts\python.exe"
set "ARQUIVO_IPYNB=04_KRONA_ORCAMENTO_ESTAT_DADOS_PAINEL.ipynb"
set "ARQUIVO_PY=04_KRONA_ORCAMENTO_ESTAT_DADOS_PAINEL.py"

echo.
echo ============================================================
echo EXECUCAO DO ORCAMENTO ESTATISTICO - DADOS PARA O PAINEL
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
echo Notebook:
echo %PASTA_SCRIPTS%\%ARQUIVO_IPYNB%
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

if not exist "%PASTA_SCRIPTS%\%ARQUIVO_IPYNB%" (
    echo ============================================================
    echo ERRO: NOTEBOOK NAO ENCONTRADO
    echo ============================================================
    echo.
    echo Caminho procurado:
    echo %PASTA_SCRIPTS%\%ARQUIVO_IPYNB%
    echo.
    pause
    exit /b 1
)

echo ============================================================
echo GERANDO ARQUIVO PY ATUALIZADO
echo ============================================================
echo.

"%PYTHON_EXE%" -m jupyter nbconvert --to script "%PASTA_SCRIPTS%\%ARQUIVO_IPYNB%" --output-dir="%PASTA_SCRIPTS%"
set "CODIGO_CONVERSAO=%ERRORLEVEL%"

echo.

if not "%CODIGO_CONVERSAO%"=="0" (
    echo ============================================================
    echo ERRO AO GERAR O ARQUIVO PY
    echo ============================================================
    echo.
    echo Codigo do erro: %CODIGO_CONVERSAO%
    echo.
    pause
    exit /b %CODIGO_CONVERSAO%
)

if not exist "%PASTA_SCRIPTS%\%ARQUIVO_PY%" (
    echo ============================================================
    echo ERRO: ARQUIVO PYTHON NAO FOI GERADO
    echo ============================================================
    echo.
    echo Caminho esperado:
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