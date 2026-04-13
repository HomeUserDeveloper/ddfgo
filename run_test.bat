@echo off
REM Батник для тестирования ddfgo

setlocal enabledelayedexpansion

REM Проверяем наличие скомпилированного бинарного файла
if not exist "ddfgo.exe" (
    echo Ошибка: ddfgo.exe не найден. Пожалуйста, скомпилируйте проект перед тестированием.
    echo Запустите: build.bat
    pause
    exit /b 1
)

echo.
echo ====== ЗАПУСК ТЕСТИРОВАНИЯ DDFGO ======
echo Версия программы: 
ddfgo.exe -version
echo.

REM Создаём тестовую директорию с дубликатами
set TEST_DIR=test_data
if exist "%TEST_DIR%" (
    echo Очищаем старую тестовую директорию...
    rmdir /s /q "%TEST_DIR%"
)

echo Создание тестовой директории...
mkdir "%TEST_DIR%"

REM Создаём несколько тестовых файлов с одинаковым содержимым (дубликаты)
echo Создание тестовых файлов...
(
    echo This is test file content number 1
) > "%TEST_DIR%\file1.txt"

(
    echo This is test file content number 1
) > "%TEST_DIR%\file2.txt"

(
    echo This is test file content number 1
) > "%TEST_DIR%\file3.txt"

(
    echo Different content here
) > "%TEST_DIR%\file4.txt"

REM Создаём подпапку с дополнительными файлами
mkdir "%TEST_DIR%\subfolder"

(
    echo This is test file content number 1
) > "%TEST_DIR%\subfolder\file5.txt"

(
    echo Another different content
) > "%TEST_DIR%\subfolder\file6.txt"

echo.
echo ====== ТЕСТОВЫЕ ФАЙЛЫ СОЗДАНЫ ======
echo Количество файлов в тестовой директории: 6
echo Ожидаемые дубликаты: file1.txt, file2.txt, file3.txt, subfolder\file5.txt (4 копии одного файла)
echo.

REM Запускаем программу в тестовом режиме
echo.
echo ====== ЗАПУСК ПРОГРАММЫ (тестовый режим) ======
echo.

ddfgo.exe -dir "%TEST_DIR%" -test -clean

echo.
echo ====== ТЕСТИРОВАНИЕ ЗАВЕРШЕНО ======
echo.
echo Результаты можно просмотреть в файлах:
echo  - ddfgo.log (общий лог)
echo  - ddfgoerr.log (лог ошибок)
echo.

pause
