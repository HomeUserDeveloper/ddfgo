# Встраивание версии в Windows ресурсы ddfgo

## Текущее состояние

Версия **встроена в Go код** и доступна через флаг:
```
ddfgo.exe -version
ddfgo.exe -v
```

Выведет: `ddfgo версия: 000.000.000.0009`

## Проблема

Версия не отображается в свойствах файла (вкладка "Подробно" -> "Версия продукта") потому что требует встраивания в Windows ресурсы (VERSIONINFO).

## Решение для Linux/macOS разработчиков

На системах с установленным `gcc` и `windres`:

```bash
# 1. Скомпилировать RC файл в объектный файл
windres -i versioninfo.rc -o rsrc_windows_amd64.o

# 2. Скомпилировать Go проект
go build -o ddfgo.exe ddfgo.go
```

## Решение для Windows с MinGW

```cmd
# 1. Установить LLVM MinGW через winget
winget install MartinStorsjo.LLVM-MinGW

# 2. Добавить примечание в PATH

# 3. Скомпилировать используя скрипт

# Или использовать goversioninfo:
goversioninfo -ver-major 0 -ver-minor 0 -ver-patch 0 -ver-build 9 ^
  -product-ver-major 0 -product-ver-minor 0 -product-ver-patch 0 -product-ver-build 9 ^
  -file-version "0.0.0.9" -product-version "0.0.0.9" ^
  -product-name "ddfgo" -company "ddfgo" ^
  -description "Duplicate Finder and File Remover" ^
  -copyright "Copyright (c) 2026 ddfgo contributors"

go build -o ddfgo.exe ddfgo.go
```

## Альтернативное решение

Использовать `rcedit` (требует Node.js):

```cmd
npm install -g rcedit
rcedit ddfgo.exe --set-version-string ProductVersion "0.0.0.9"
rcedit ddfgo.exe --set-version-string FileVersion "0.0.0.9"
rcedit ddfgo.exe --set-version-string ProductName "ddfgo"
rcedit ddfgo.exe --set-version-string CompanyName "ddfgo"
```

## Файлы конфигурации

В репозитории находятся конфигурации для встраивания версии:

- `versioninfo.json` - для goversioninfo
- `versioninfo.rc` - для windres/rsrc
- `winres/winres.json` - для go-winres

## Рекомендация

Для большинства пользователей текущая реализация (версия через флаг `-version`) достаточна и не требует Windows ресурсов.

Встраивание версии в ресурсы необходимо только если требуется видеть версию в свойствах файла через проводник Windows.
