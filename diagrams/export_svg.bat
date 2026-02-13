@echo off
REM drawioファイルを一括SVGエクスポートするスクリプト
REM 使い方: diagrams フォルダ内で export_svg.bat を実行

setlocal enabledelayedexpansion

set "DRAWIO=C:\Program Files\draw.io\draw.io.exe"
set "DIR=%~dp0"

echo === draw.io SVG Export ===
echo.

set count=0
for %%f in ("%DIR%*.drawio") do (
    set /a count+=1
    set "name=%%~nf"
    echo [!count!] %%~nxf -^> !name!.svg
    "%DRAWIO%" --export --format svg --embed-diagram --output "%DIR%!name!.svg" "%%f"
)

echo.
echo === %count% files exported ===
pause