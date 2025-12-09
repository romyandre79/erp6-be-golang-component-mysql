@echo off
setlocal enabledelayedexpansion

REM ===========================
REM READ CURRENT VERSION
REM ===========================
for /f "tokens=2 delims=:," %%a in ('findstr /i "version" plugin.json') do (
    set raw=%%a
)

set raw=%raw:"=%
set version=%raw: =%

echo Current version: %version%

for /f "tokens=1,2,3 delims=." %%a in ("%version%") do (
    set major=%%a
    set minor=%%b
    set patch=%%c
)

set /a patch+=1
set new_version=%major%.%minor%.%patch%
echo New version: %new_version%

REM ===========================
REM UPDATE plugin.json
REM ===========================
powershell -Command "(Get-Content plugin.json) -replace '\"version\": \"[0-9.]+\"', '\"version\": \"%new_version%\"' | Set-Content plugin.json"

REM ===========================
REM CLEAN BUILD FOLDER
REM ===========================
echo Cleaning build folder...
if exist build rmdir /s /q build
mkdir build

REM ===========================
REM MULTI OS BUILD
REM ===========================
set TARGETS=windows/amd64 windows/arm64 linux/amd64 linux/arm64 darwin/amd64 darwin/arm64

echo Building for all OS...

for %%T in (%TARGETS%) do (
    for /f "tokens=1,2 delims=/" %%a in ("%%T") do (
        set GOOS=%%a
        set GOARCH=%%b

        if "%%a"=="windows" (
            go build -o build/mysql_%%a_%%b.exe main.go
        ) else (
            go build -o build/mysql_%%a_%%b main.go
        )
    )
)

REM ===========================
REM CREATE ZIP
REM ===========================
set ZIPFILE=mysql_plugin_v%new_version%.zip
powershell Compress-Archive -Path plugin.json,build\* -DestinationPath %ZIPFILE% -Force

REM CLEAN BUILD
rmdir /s /q build

REM ===========================
REM MOVE TO dist/
REM ===========================
if not exist ..\erp6-be-golang-component-dist\mysql mkdir ..\erp6-be-golang-component-dist\mysql
move %ZIPFILE% ..\erp6-be-golang-component-dist\mysql\

REM ===========================
REM GENERATE CHECKSUM
REM ===========================
certutil -hashfile ..\erp6-be-golang-component-dist\mysql\%ZIPFILE% SHA256 > ..\erp6-be-golang-component-dist\mysql\checksums_v%new_version%.txt

REM REMOVE unnecessary lines from certutil output
powershell -Command "(Get-Content ..\erp6-be-golang-component-dist\mysql\checksums_v%new_version%.txt | Select-Object -Skip 1 | Select-Object -SkipLast 1) | Set-Content ..\erp6-be-golang-component-dist\mysql\checksums_v%new_version%.txt"

REM ===========================
REM UPDATE CHANGELOG
REM ===========================
echo Writing CHANGELOG.md...

set change="Auto build version %new_version%"

(
    echo ## v%new_version% - %date%
    echo - %change%
    echo.
    type CHANGELOG.md 2>nul
) > CHANGELOG.tmp

move /y CHANGELOG.tmp CHANGELOG.md

echo.
echo Build complete → ..\erp6-be-golang-component-dist\mysql\%ZIPFILE%
echo Checksum file → ..\erp6-be-golang-component-dist\mysql\checksums_v%new_version%.txt
echo.
