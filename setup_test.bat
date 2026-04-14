@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

REM Create test directory
if exist test_sf_check rmdir /s /q test_sf_check
mkdir test_sf_check

REM Create small duplicate files (< 10KB)
(
    echo small_content
) > test_sf_check\small1.txt
(
    echo small_content
) > test_sf_check\small2.txt

REM Create large duplicate files (> 10KB) - each line makes it bigger
setlocal enabledelayedexpansion
set "large="
for /L %%i in (1,1,500) do (
    set "large=!large!This is a large file content line number %%i with some additional text to make it bigger and exceed 10KB threshold. "
)
(
    echo !large!
) > test_sf_check\large1.txt
(
    echo !large!
) > test_sf_check\large2.txt

REM Create empty subdirectory
mkdir test_sf_check\empty_dir

echo Test directory created successfully!
echo.
echo File sizes:
for %%F in (test_sf_check\*.txt) do (
    for /F "tokens=*" %%A in ('powershell -Command "(Get-Item '%%F').Length"') do (
        echo %%F: %%A bytes
    )
)

