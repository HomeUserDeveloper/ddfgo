@echo off
chcp 65001 >nul
REM Скрипт для компиляции ddfgo с автоматическим увеличением номера версии
REM Формат версии: 000.000.000.000 (Major.Minor.Patch.Build)

setlocal enabledelayedexpansion

echo.
echo ====== BUILD SYSTEM для ddfgo ======
echo.

REM Проверяем наличие файла версии
if not exist "version.txt" (
    echo Создаю файл версии...
    echo 000.000.000.001 > version.txt
)

REM Читаем текущую версию
set /p CURRENT_VERSION=<version.txt

echo Текущая версия: %CURRENT_VERSION%

REM Парсим версию
for /f "tokens=1,2,3,4 delims=." %%a in ("%CURRENT_VERSION%") do (
    set MAJOR=%%a
    set MINOR=%%b
    set PATCH=%%c
    set BUILD=%%d
)

REM Увеличиваем BUILD номер
set /a BUILD_NEW=!BUILD! + 1

REM Форматируем обратно с ведущими нулями
REM Используем PowerShell для форматирования с ведущими нулями
for /f "delims=" %%a in ('powershell -NoProfile -Command "'{0:D3}.{1:D3}.{2:D3}.{3:D4}' -f !MAJOR!,!MINOR!,!PATCH!,!BUILD_NEW!" 2^>nul') do set NEW_VERSION=%%a

echo Новая версия: !NEW_VERSION!

REM Сохраняем новую версию
echo !NEW_VERSION! > version.txt

echo.
echo ====== КОМПИЛЯЦИЯ ======
echo.

REM Проверяем наличие Go
where go >nul 2>&1
if errorlevel 1 (
    echo Ошибка: Go не установлен или не добавлен в PATH
    echo Пожалуйста, установите Go с https://golang.org/
    pause
    exit /b 1
)

REM Компилируем проект с встраиванием версии через ldflags
echo Компиляция ddfgo версия !NEW_VERSION!...
go build -ldflags "-X main.Version=!NEW_VERSION!" -o ddfgo.exe

if errorlevel 1 (
    echo.
    echo ОШИБКА: Компиляция не удалась!
    echo Версия была изменена на: !NEW_VERSION!
    pause
    exit /b 1
)

echo.
echo ====== КОМПИЛЯЦИЯ УСПЕШНА ======
echo.
echo Файл: ddfgo.exe
echo Версия: !NEW_VERSION!
echo Время: %date% %time%
echo.

REM Показываем информацию о скомпилированном файле
echo Информация о файле:
for %%A in (ddfgo.exe) do (
    echo  Размер: %%~zA байт
    echo  Путь: %%~fA
)

echo.
echo Версия встроена в исполняемый файл. Проверьте:
echo   ddfgo.exe -version
echo.

pause
