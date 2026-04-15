// MIT License
//
// Copyright (c) 2026 ddfgo contributors
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/zeebo/xxh3"
	_ "modernc.org/sqlite"
)

const (
	lockFileName = "ddfgo.lock"
)

// expandToSkip - расширения файлов которые исключаются по умолчанию
// (проблемные файлы для обычных пользователей - Windows Defender блокирует доступ)
var expandToSkip = map[string]bool{
	".exe": true,
	".dll": true,
	".lib": true,
}

// Version встраивается при компиляции через ldflags
var Version = "000.000.000.0009"

var (
	filesProcessed  int
	filesRemoved    int
	duplicatesFound int
	filesSkipped    int    // количество пропущенных файлов (исключенные расширения)
	logFile         string = "ddfgo.log"
	errLogFile      string = "ddfgoerr.log"
	testMode        bool
	cleanMode       bool
	removeAllFiles  bool // флаг -all для удаления всех дубликатов, включая маленькие файлы
	removeEmptyDirs bool // флаг -dir0 для удаления пустых каталогов
	extensionAll    bool // флаг -ext-all для обработки файлов со всеми расширениями
	cpuProfilePath  string
	memProfilePath  string
	logOutput       *os.File
)

func main() {
	os.Exit(run())
}

func run() (exitCode int) {
	var targetDir string = os.Getenv("DDF_DIR")
	if targetDir == "" {
		targetDir = "E:\\SortDir"
	}

	var showHelp bool
	var showVersion bool

	flag.StringVar(&targetDir, "dir", targetDir, "Директория для сканирования на наличие дубликатов")
	flag.BoolVar(&testMode, "test", false, "Записать результаты в лог файл")
	flag.BoolVar(&cleanMode, "clean", false, "Очистить лог файл перед запуском")
	flag.BoolVar(&removeAllFiles, "all", false, "Удалять дубликаты всех размеров, включая файлы < 10KB")
	flag.BoolVar(&removeEmptyDirs, "dir0", false, "Удалить пустые каталоги после завершения")
	flag.BoolVar(&extensionAll, "ext-all", false, "Обрабатывать файлы со всеми расширениями (включая .exe, .dll, .lib)")
	flag.BoolVar(&showHelp, "help", false, "Показать справку")
	flag.BoolVar(&showHelp, "h", false, "Показать справку (сокращенно)")
	flag.BoolVar(&showVersion, "version", false, "Показать номер версии")
	flag.BoolVar(&showVersion, "v", false, "Показать номер версии (сокращенно)")
	flag.StringVar(&cpuProfilePath, "cpuprofile", "", "Сохранить CPU профиль в файл")
	flag.StringVar(&memProfilePath, "memprofile", "", "Сохранить профиль памяти в файл")
	flag.Parse()

	// Обработка флагов справки и версии
	if showHelp || (len(os.Args) == 1) {
		printHelp()
		return 0
	}

	if showVersion {
		fmt.Printf("ddfgo версия: %s\n", Version)
		return 0
	}

	// Инициализируем логирование если включен тестовый режим
	if testMode {
		// Если флаг -clean, удаляем старые лог файлы
		if cleanMode {
			os.Remove(logFile)
			os.Remove(errLogFile)
		}

		// Открываем лог файл для добавления информации
		var err error
		logOutput, err = os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
		if err != nil {
			fmt.Printf("Ошибка открытия лог файла: %v\n", err)
			return 1
		}
		defer logOutput.Close()

		logPrintf("===== Начало нового запуска программы ddfgo =====\n")
		logPrintf("Время: %v\n", time.Now().Format("2006-01-02 15:04:05"))
		logPrintf("Директория: %s\n\n", targetDir)
	} else {
		// Даже без тестового режима, если указан флаг -clean, очищаем файлы ошибок
		if cleanMode {
			os.Remove(errLogFile)
		}
	}

	if targetDir == "" || !dirExists(targetDir) {
		msg := "Директория не найдена или не указана"
		logError(msg)
		logPrintf("Ошибка: %s\n", msg)
		return 1
	}

	// Проверка MUTEX - предотвращение одновременного запуска
	lockFile := filepath.Join(os.TempDir(), lockFileName)
	lockHandle, err := acquireLock(lockFile)
	if err != nil {
		msg := "Другой экземпляр уже запущен"
		logError(msg)
		logPrintf("Ошибка: %s\n", msg)
		return 1
	}
	defer releaseLock(lockFile, lockHandle)

	if cpuProfilePath != "" {
		cpuFile, err := startCPUProfiling(cpuProfilePath)
		if err != nil {
			logError("Ошибка запуска CPU профилирования: %v", err)
			logPrintf("Ошибка запуска CPU профилирования: %v\n", err)
			return 1
		}
		defer func() {
			pprof.StopCPUProfile()
			cpuFile.Close()
			logPrintf("CPU профиль сохранен: %s\n", cpuProfilePath)
		}()
	}

	if memProfilePath != "" {
		defer func() {
			if err := writeMemoryProfile(memProfilePath); err != nil {
				logError("Ошибка сохранения профиля памяти: %v", err)
				logPrintf("Ошибка сохранения профиля памяти: %v\n", err)
				if exitCode == 0 {
					exitCode = 1
				}
				return
			}
			logPrintf("Профиль памяти сохранен: %s\n", memProfilePath)
		}()
	}

	// Выполняем основную логику
	exitCode = runApp(targetDir, lockFile)
	return exitCode
}

func startCPUProfiling(path string) (*os.File, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	if err := pprof.StartCPUProfile(file); err != nil {
		file.Close()
		return nil, err
	}

	return file, nil
}

func writeMemoryProfile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	runtime.GC()
	return pprof.WriteHeapProfile(file)
}

// printHelp выводит справку по использованию программы
func printHelp() {
	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════════════════")
	fmt.Println("  ddfgo - Утилита поиска и удаления дубликатов файлов")
	fmt.Println("  Версия:", Version)
	fmt.Println("═══════════════════════════════════════════════════════════════════")
	fmt.Println()
	fmt.Println("ИСПОЛЬЗОВАНИЕ:")
	fmt.Println("  ddfgo -dir \"путь\" [параметры]")
	fmt.Println()
	fmt.Println("ОСНОВНЫЕ ПАРАМЕТРЫ:")
	fmt.Println()
	fmt.Println("  -dir \"путь\"")
	fmt.Println("    Директория для сканирования на наличие дубликатов.")
	fmt.Println("    Если не указана, используется значение переменной окружения DDF_DIR")
	fmt.Println("    или стандартная директория E:\\SortDir")
	fmt.Println("    Пример: ddfgo -dir \"D:\\Downloads\"")
	fmt.Println()
	fmt.Println("  -test")
	fmt.Println("    Тестовый режим: файлы НЕ удаляются, результаты записываются в ddfgo.log")
	fmt.Println("    Используйте для проверки до реального удаления дубликатов")
	fmt.Println("    Пример: ddfgo -dir \"D:\\Photos\" -test")
	fmt.Println()
	fmt.Println("  -clean")
	fmt.Println("    Очистить старые лог файлы перед запуском")
	fmt.Println("    Удаляет ddfgo.log и ddfgoerr.log перед работой")
	fmt.Println("    Пример: ddfgo -dir \"D:\\Videos\" -test -clean")
	fmt.Println()
	fmt.Println("  -all")
	fmt.Println("    Удалять дубликаты всех размеров, включая файлы < 10KB")
	fmt.Println("    По умолчанию файлы меньше 10KB исключаются из удаления")
	fmt.Println("    (это полезно при наличии генерируемых файлов в электронных книгах)")
	fmt.Println("    Пример: ddfgo -dir \"D:\\Books\" -all")
	fmt.Println()
	fmt.Println("  -dir0")
	fmt.Println("    Удалить пустые каталоги после завершения обработки")
	fmt.Println("    Рекурсивно обходит все подпапки и удаляет пустые директории")
	fmt.Println("    Пример: ddfgo -dir \"D:\\Files\" -dir0")
	fmt.Println()
	fmt.Println("  -ext-all")
	fmt.Println("    Обрабатывать файлы со всеми расширениями")
	fmt.Println("    По умолчанию исключаются: .exe, .dll, .lib")
	fmt.Println("    (эти файлы могут быть заблокированы Windows Defender при обычных правах)")
	fmt.Println("    Пример: ddfgo -dir \"D:\\Data\" -ext-all")
	fmt.Println()
	fmt.Println("  -cpuprofile \"файл\"")
	fmt.Println("    Сохранить CPU профиль выполнения в указанный файл")
	fmt.Println("    Пример: ddfgo -dir \"D:\\Data\" -test -cpuprofile cpu.prof")
	fmt.Println()
	fmt.Println("  -memprofile \"файл\"")
	fmt.Println("    Сохранить профиль памяти (heap) в указанный файл")
	fmt.Println("    Пример: ddfgo -dir \"D:\\Data\" -test -memprofile mem.prof")
	fmt.Println()
	fmt.Println("ИНФОРМАЦИОННЫЕ ПАРАМЕТРЫ:")
	fmt.Println()
	fmt.Println("  -help, -h")
	fmt.Println("    Показать эту справку")
	fmt.Println("    Пример: ddfgo -help")
	fmt.Println()
	fmt.Println("  -version, -v")
	fmt.Println("    Показать номер версии")
	fmt.Println("    Пример: ddfgo -version")
	fmt.Println()
	fmt.Println("ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ:")
	fmt.Println()
	fmt.Println("  1. Показать справку:")
	fmt.Println("     ddfgo")
	fmt.Println("     ddfgo -help")
	fmt.Println("     ddfgo -h")
	fmt.Println()
	fmt.Println("  2. Показать версию:")
	fmt.Println("     ddfgo -version")
	fmt.Println("     ddfgo -v")
	fmt.Println()
	fmt.Println("  3. Простое сканирование и удаление дубликатов:")
	fmt.Println("     ddfgo -dir \"D:\\Downloads\"")
	fmt.Println()
	fmt.Println("  4. Сканирование в режиме тестирования (без удаления):")
	fmt.Println("     ddfgo -dir \"E:\\Photos\" -test -clean")
	fmt.Println("     Затем посмотрите результаты: type ddfgo.log")
	fmt.Println()
	fmt.Println("  5. Удаление дубликатов всех размеров (включая маленькие файлы):")
	fmt.Println("     ddfgo -dir \"D:\\Books\" -all")
	fmt.Println()
	fmt.Println("  6. Удаление дубликатов с очисткой пустых каталогов:")
	fmt.Println("     ddfgo -dir \"D:\\Files\" -dir0")
	fmt.Println()
	fmt.Println("  7. Обработка файлов со всеми расширениями:")
	fmt.Println("     ddfgo -dir \"D:\\Data\" -ext-all")
	fmt.Println()
	fmt.Println("  7. Комбинированное использование всех флагов:")
	fmt.Println("     ddfgo -dir \"D:\\Data\" -all -dir0 -ext-all -test -clean")
	fmt.Println()
	fmt.Println("  8. Используя переменную окружения:")
	fmt.Println("     set DDF_DIR=D:\\MyFiles")
	fmt.Println("     ddfgo")
	fmt.Println()
	fmt.Println("ОПИСАНИЕ:")
	fmt.Println()
	fmt.Println("  ddfgo - высокопроизводительная утилита для поиска и удаления")
	fmt.Println("  дубликатов файлов в больших каталогах. Использует:")
	fmt.Println()
	fmt.Println("  • SQLite базу данных для быстрого поиска")
	fmt.Println("  • XXH3 хеширование для надежного определения дубликатов")
	fmt.Println("  • Параллельную обработку (адаптируется к количеству CPU: NumCPU() * 2 горутин)")
	fmt.Println("  • Механизм блокировки для предотвращения одновременных запусков")
	fmt.Println()
	fmt.Println("ИСКЛЮЧАЕМЫЕ РАСШИРЕНИЯ (по умолчанию):")
	fmt.Println()
	fmt.Println("  .exe - Исполняемые файлы")
	fmt.Println("  .dll - Динамические библиотеки")
	fmt.Println("  .lib - Библиотеки ссылок")
	fmt.Println()
	fmt.Println("  Windows Defender может блокировать доступ к этим файлам")
	fmt.Println("  при запуске под обычным пользователем.")
	fmt.Println("  Используйте флаг -ext-all для обработки всех файлов.")
	fmt.Println()
	fmt.Println("ЛОГИРОВАНИЕ:")
	fmt.Println()
	fmt.Println("  ddfgo.log    - Основной лог (создается с флагом -test)")
	fmt.Println("  ddfgoerr.log - Лог ошибок (создается при ошибках)")
	fmt.Println()
	fmt.Println("БЕЗОПАСНОСТЬ:")
	fmt.Println()
	fmt.Println("  ⚠️  ВНИМАНИЕ: Удаленные файлы НЕВОЗМОЖНО восстановить!")
	fmt.Println()
	fmt.Println("  Перед первым использованием:")
	fmt.Println("  1. Сделайте резервную копию данных")
	fmt.Println("  2. Запустите в режиме тестирования: ddfgo -dir \"...\" -test -clean")
	fmt.Println("  3. Проверьте логи: type ddfgo.log")
	fmt.Println("  4. Только если результаты корректны, запустите реально")
	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════════════════")
	fmt.Println()
}

// logPrintf выводит сообщение в консоль и/или в лог файл в зависимости от режима
func logPrintf(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)

	// Всегда выводим в консоль
	fmt.Print(message)

	// Если в тестовом режиме, пишем также в лог файл
	if testMode && logOutput != nil {
		fmt.Fprint(logOutput, message)
	}
}

// logError логирует критическую ошибку в файл ddfgoerr.log
func logError(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	fullMessage := fmt.Sprintf("[%s] %s\n", timestamp, message)

	// Всегда выводим в консоль
	fmt.Print(fullMessage)

	// Открываем файл ошибок и добавляем сообщение
	file, err := os.OpenFile(errLogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Printf("Ошибка открытия файла ошибок: %v\n", err)
		return
	}
	defer file.Close()

	fmt.Fprint(file, fullMessage)
}

// runApp выполняет основную логику приложения и гарантирует выполнение defer
func runApp(targetDir string, lockFile string) int {
	// Запоминаем время начала работы
	startTime := time.Now()

	// Создание временного SQLite файла
	dbFile := filepath.Join(os.TempDir(), "ddfgo_"+fmt.Sprintf("%d", time.Now().UnixNano())+".db")
	defer os.Remove(dbFile)

	db, err := sql.Open("sqlite", dbFile)
	if err != nil {
		logError("Ошибка открытия базы данных: %v", err)
		logPrintf("Ошибка открытия базы данных: %v\n", err)
		return 1
	}
	defer db.Close()

	if err := initDatabase(db); err != nil {
		logError("Ошибка инициализации базы данных: %v", err)
		logPrintf("Ошибка инициализации базы данных: %v\n", err)
		return 1
	}

	logPrintf("Сканирование директории: %s\n", targetDir)
	logPrintf("База данных: %s\n", dbFile)

	// Шаг 1: Сканирование директории и добавление файлов
	if err := scanDirectory(targetDir, db); err != nil {
		logError("Ошибка сканирования директории: %v", err)
		logPrintf("Ошибка сканирования директории: %v\n", err)
		return 1
	}

	// Шаг 2: Нахождение дубликатов по размеру
	if err := findDuplicatesBySize(db); err != nil {
		logError("Ошибка поиска дубликатов по размеру: %v", err)
		logPrintf("Ошибка поиска дубликатов по размеру: %v\n", err)
		return 1
	}

	// Шаг 3: Вычисление XXH3 хешей для потенциальных дубликатов
	if err := calculateHashes(db); err != nil {
		logError("Ошибка вычисления XXH3 хешей: %v", err)
		logPrintf("Ошибка вычисления XXH3 хешей: %v\n", err)
		return 1
	}

	// Шаг 4: Нахождение реальных дубликатов по XXH3
	if err := findDuplicatesByHash(db); err != nil {
		logError("Ошибка поиска дубликатов по XXH3: %v", err)
		logPrintf("Ошибка поиска дубликатов по XXH3: %v\n", err)
		return 1
	}

	// Шаг 5: Пометка и удаление дубликатов
	if err := markAndRemoveDuplicates(db); err != nil {
		logError("Ошибка удаления дубликатов: %v", err)
		logPrintf("Ошибка удаления дубликатов: %v\n", err)
		return 1
	}

	// Шаг 6: Удаление пустых каталогов (если включен флаг -dir0)
	if removeEmptyDirs {
		logPrintf("\nУдаление пустых каталогов...\n")
		removedDirs := removeEmptyDirectories(targetDir)
		logPrintf("Удалено пустых каталогов: %d\n", removedDirs)
	}

	// Вычисляем прошедшее время
	elapsed := time.Since(startTime)

	// Вывод статистики
	logPrintf("\n=== Статистика ===\n")
	logPrintf("Файлов обработано: %d\n", filesProcessed)
	if !extensionAll {
		logPrintf("Файлов пропущено (исключенные расширения): %d\n", filesSkipped)
	}
	logPrintf("Найдено дубликатов: %d\n", duplicatesFound)
	logPrintf("Файлов удалено: %d\n", filesRemoved)
	logPrintf("Время работы: %v\n", elapsed)

	return 0
}

// initDatabase создаёт таблицу для хранения информации о файлах
// Улучшенная версия initDatabase
// Исправленная версия initDatabase
func initDatabase(db *sql.DB) error {
	// Настройки базы данных (до любых транзакций)
	settings := []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
		"PRAGMA cache_size = 100000",
		"PRAGMA temp_store = MEMORY",
		"PRAGMA busy_timeout = 30000",
		"PRAGMA mmap_size = 268435456", // 256MB
	}

	for _, setting := range settings {
		if _, err := db.Exec(setting); err != nil {
			return fmt.Errorf("ошибка настройки SQLite %s: %v", setting, err)
		}
	}

	// Создание таблицы и индексов
	createTableQuery := `
    CREATE TABLE IF NOT EXISTS curfiles (
        fnum INTEGER PRIMARY KEY,
        fname TEXT NOT NULL UNIQUE,
        fsize INTEGER NOT NULL,
        quick_hash TEXT,
        full_hash TEXT,
        fflag INTEGER DEFAULT 0
    )`

	if _, err := db.Exec(createTableQuery); err != nil {
		return fmt.Errorf("ошибка создания таблицы: %v", err)
	}

	// Создание индексов отдельно
	indexes := []string{
		"CREATE INDEX IF NOT EXISTS idx_fsize ON curfiles(fsize)",
		"CREATE INDEX IF NOT EXISTS idx_quick_hash ON curfiles(quick_hash)",
		"CREATE INDEX IF NOT EXISTS idx_full_hash ON curfiles(full_hash)",
		"CREATE INDEX IF NOT EXISTS idx_fflag ON curfiles(fflag)",
	}

	for _, indexQuery := range indexes {
		if _, err := db.Exec(indexQuery); err != nil {
			return fmt.Errorf("ошибка создания индекса: %v", err)
		}
	}

	return nil
}

// Оптимизированная вставка файлов с пакетной обработкой
func scanDirectory(root string, db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("INSERT OR IGNORE INTO curfiles (fname, fsize) VALUES (?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	batchCount := 0
	const batchSize = 1000

	err = filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		if !info.IsDir() {
			// Проверяем расширение файла
			if !extensionAll {
				ext := strings.ToLower(filepath.Ext(path))
				if expandToSkip[ext] {
					filesSkipped++
					return nil // Пропускаем файл с исключаемым расширением
				}
			}

			if _, err := stmt.Exec(path, info.Size()); err != nil {
				fmt.Printf("Ошибка добавления файла %s: %v\n", path, err)
			} else {
				filesProcessed++
				batchCount++

				// Коммитим каждые 1000 записей
				if batchCount >= batchSize {
					if err := tx.Commit(); err != nil {
						return err
					}
					tx, err = db.Begin()
					if err != nil {
						return err
					}
					stmt, err = tx.Prepare("INSERT OR IGNORE INTO curfiles (fname, fsize) VALUES (?, ?)")
					if err != nil {
						return err
					}
					batchCount = 0
				}
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	return tx.Commit()
}

// findDuplicatesBySize оставляет только файлы, размер которых встречается более одного раза
// Оптимизированный поиск дубликатов по размеру
func findDuplicatesBySize(db *sql.DB) error {
	queries := []string{
		`CREATE TEMPORARY TABLE temp_duplicates AS
         SELECT fnum, fname, fsize 
         FROM curfiles 
         WHERE fsize IN (
             SELECT fsize 
             FROM curfiles 
             GROUP BY fsize 
             HAVING COUNT(*) > 1
         )`,
		`DELETE FROM curfiles`,
		`INSERT INTO curfiles (fnum, fname, fsize)
         SELECT fnum, fname, fsize FROM temp_duplicates`,
		`DROP TABLE temp_duplicates`,
	}

	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			return err
		}
	}

	return nil
}

// calculateHashes вычисляет XXH3 хеши для всех файлов в таблице с оптимизацией
// Сначала быстрый хеш (5% начала + 5% конца), затем полный хеш только для потенциальных дубликатов
func calculateHashes(db *sql.DB) error {
	// Шаг 1: Вычисляем быстрый хеш для всех файлов
	if err := calculateQuickHashes(db); err != nil {
		return err
	}

	// Шаг 2: Находим файлы с одинаковым быстрым хешем и вычисляем полный хеш
	if err := calculateFullHashesForDuplicates(db); err != nil {
		return err
	}

	return nil
}

// calculateQuickHashes вычисляет быстрый хеш для всех файлов
func calculateQuickHashes(db *sql.DB) error {
	rows, err := db.Query("SELECT fnum, fname, fsize FROM curfiles WHERE quick_hash IS NULL")
	if err != nil {
		return err
	}
	defer rows.Close()

	type hashResult struct {
		fnum      int64
		quickHash string
		err       error
	}

	results := make(chan hashResult, 100)
	var wg sync.WaitGroup

	var files []struct {
		fnum  int64
		fname string
		fsize int64
	}
	for rows.Next() {
		var fnum int64
		var fname string
		var fsize int64
		if err := rows.Scan(&fnum, &fname, &fsize); err != nil {
			continue
		}
		files = append(files, struct {
			fnum  int64
			fname string
			fsize int64
		}{fnum, fname, fsize})
	}

	// Используем адаптивное количество воркеров
	numWorkers := calculateOptimalWorkerCount(int64(len(files)))
	semaphore := make(chan struct{}, numWorkers)
	logPrintf("Вычисление быстрых хешей: использование %d воркеров для %d файлов\n", numWorkers, len(files))

	for _, file := range files {
		if !fileExists(file.fname) {
			continue
		}

		wg.Add(1)
		go func(fnum int64, fname string, fsize int64) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			quickHash, err := quickXXH3File(fname, fsize)
			results <- hashResult{fnum: fnum, quickHash: quickHash, err: err}
		}(file.fnum, file.fname, file.fsize)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("UPDATE curfiles SET quick_hash = ? WHERE fnum = ?")
	if err != nil {
		return err
	}
	defer stmt.Close()

	batchCount := 0
	for result := range results {
		if result.err != nil {
			fmt.Printf("Ошибка вычисления быстрого хеша: %v\n", result.err)
			continue
		}

		if _, err := stmt.Exec(result.quickHash, result.fnum); err != nil {
			fmt.Printf("Ошибка обновления быстрого хеша: %v\n", err)
			continue
		}

		batchCount++
		if batchCount >= 100 {
			if err := tx.Commit(); err != nil {
				return err
			}
			tx, err = db.Begin()
			if err != nil {
				return err
			}
			stmt, err = tx.Prepare("UPDATE curfiles SET quick_hash = ? WHERE fnum = ?")
			if err != nil {
				return err
			}
			batchCount = 0
		}
	}

	return tx.Commit()
}

// calculateFullHashesForDuplicates вычисляет полный хеш только для файлов с одинаковым быстрым хешем
func calculateFullHashesForDuplicates(db *sql.DB) error {
	// Находим файлы, где quick_hash повторяется более одного раза
	rows, err := db.Query(`
		SELECT fnum, fname FROM curfiles 
		WHERE quick_hash IS NOT NULL AND full_hash IS NULL AND quick_hash IN (
			SELECT quick_hash FROM curfiles 
			WHERE quick_hash IS NOT NULL
			GROUP BY quick_hash 
			HAVING COUNT(*) > 1
		)
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	type hashResult struct {
		fnum     int64
		fullHash string
		err      error
	}

	results := make(chan hashResult, 100)
	var wg sync.WaitGroup

	var files []struct {
		fnum  int64
		fname string
	}
	for rows.Next() {
		var fnum int64
		var fname string
		if err := rows.Scan(&fnum, &fname); err != nil {
			continue
		}
		files = append(files, struct {
			fnum  int64
			fname string
		}{fnum, fname})
	}

	// Для full hash используем отдельную стратегию, чтобы не упираться в 1 воркер.
	numWorkers := calculateFullHashWorkerCount(int64(len(files)))
	semaphore := make(chan struct{}, numWorkers)
	logPrintf("Вычисление полных хешей: использование %d воркеров для %d файлов\n", numWorkers, len(files))

	for _, file := range files {
		if !fileExists(file.fname) {
			continue
		}

		wg.Add(1)
		go func(fnum int64, fname string) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			fullHash, err := fullXXH3File(fname)
			results <- hashResult{fnum: fnum, fullHash: fullHash, err: err}
		}(file.fnum, file.fname)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("UPDATE curfiles SET full_hash = ? WHERE fnum = ?")
	if err != nil {
		return err
	}
	defer stmt.Close()

	batchCount := 0
	for result := range results {
		if result.err != nil {
			fmt.Printf("Ошибка вычисления полного хеша: %v\n", result.err)
			continue
		}

		if _, err := stmt.Exec(result.fullHash, result.fnum); err != nil {
			fmt.Printf("Ошибка обновления полного хеша: %v\n", err)
			continue
		}

		batchCount++
		if batchCount >= 100 {
			if err := tx.Commit(); err != nil {
				return err
			}
			tx, err = db.Begin()
			if err != nil {
				return err
			}
			stmt, err = tx.Prepare("UPDATE curfiles SET full_hash = ? WHERE fnum = ?")
			if err != nil {
				return err
			}
			batchCount = 0
		}
	}

	return tx.Commit()
}

// findDuplicatesByHash оставляет только файлы с повторяющимися full_hash
func findDuplicatesByHash(db *sql.DB) error {
	var dupGroupCount int64
	err := db.QueryRow(`
		SELECT COUNT(*)
		FROM (
			SELECT full_hash
			FROM curfiles
			WHERE full_hash IS NOT NULL
			GROUP BY full_hash
			HAVING COUNT(*) > 1
		)
	`).Scan(&dupGroupCount)
	if err != nil {
		return err
	}

	// Fast path: если групп дублей нет, очищаем таблицу одним запросом.
	if dupGroupCount == 0 {
		duplicatesFound = 0
		_, err = db.Exec("DELETE FROM curfiles")
		return err
	}

	// Потоковая SQL-обработка: без загрузки всех дубликатов в память Go.
	if _, err := db.Exec(`
		CREATE TEMPORARY TABLE temp_dup_hashes AS
		SELECT full_hash
		FROM curfiles
		WHERE full_hash IS NOT NULL
		GROUP BY full_hash
		HAVING COUNT(*) > 1
	`); err != nil {
		return err
	}
	defer db.Exec("DROP TABLE IF EXISTS temp_dup_hashes")

	var totalDupRows int64
	err = db.QueryRow(`
		SELECT COALESCE(SUM(cnt - 1), 0)
		FROM (
			SELECT COUNT(*) AS cnt
			FROM curfiles
			WHERE full_hash IN (SELECT full_hash FROM temp_dup_hashes)
			GROUP BY full_hash
		)
	`).Scan(&totalDupRows)
	if err != nil {
		return err
	}
	duplicatesFound = int(totalDupRows)

	// Оставляем в таблице только группы с повторяющимся full_hash.
	_, err = db.Exec(`
		DELETE FROM curfiles
		WHERE full_hash IS NULL
		   OR full_hash NOT IN (SELECT full_hash FROM temp_dup_hashes)
	`)
	if err != nil {
		return err
	}

	return nil
}

// markAndRemoveDuplicates помечает и удаляет дубликаты, оставляя один файл из каждой группы
// Исключает файлы меньше 10KB по умолчанию (если не установлен флаг -all)
func markAndRemoveDuplicates(db *sql.DB) error {
	minFileSize := int64(10240) // 10KB в байтах
	if removeAllFiles {
		minFileSize = 0 // Если флаг -all, то удаляем все, включая маленькие файлы
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Используем оконные функции для оптимизации
	query := fmt.Sprintf(`
    UPDATE curfiles 
    SET fflag = 1 
    WHERE fnum IN (
        SELECT fnum 
        FROM (
            SELECT fnum, fsize,
                   ROW_NUMBER() OVER (PARTITION BY full_hash ORDER BY fname) as rn
            FROM curfiles 
            WHERE full_hash IS NOT NULL
        ) ranked
        WHERE ranked.rn > 1 AND ranked.fsize >= %d
    )
    `, minFileSize)

	result, err := tx.Exec(query)
	if err != nil {
		return err
	}

	affected, _ := result.RowsAffected()

	// Логируем информацию об исключенных файлах
	if !removeAllFiles {
		logPrintf("Помечено для удаления: %d файлов (файлы < 10KB исключены)\n", affected)
	} else {
		logPrintf("Помечено для удаления: %d файлов (все размеры, флаг -all)\n", affected)
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	// Обработка файлов для удаления
	rows, err := db.Query("SELECT fname FROM curfiles WHERE fflag = 1")
	if err != nil {
		return err
	}
	defer rows.Close()

	removedCount := 0
	for rows.Next() {
		var fname string
		if err := rows.Scan(&fname); err != nil {
			continue
		}

		if testMode {
			// В режиме тестирования только логируем, не удаляем и не считаем
			logPrintf("Файл будет удален: %s\n", fname)
		} else {
			// В обычном режиме удаляем файл
			if err := os.Remove(fname); err != nil {
				logPrintf("Ошибка удаления файла %s: %v\n", fname, err)
			} else {
				removedCount++
				logPrintf("Файл удален: %s\n", fname)
			}
		}
	}

	filesRemoved = removedCount
	return nil
}

// calculateOptimalWorkerCount определяет оптимальное количество воркеров
// на основе количества файлов для обработки
func calculateOptimalWorkerCount(totalFiles int64) int {
	cpuCount := runtime.NumCPU()

	// Для большых количеств файлов используем разные стратегии
	switch {
	// Мало файлов - не нужно много горутин (I/O bound)
	case totalFiles < 100:
		return 1

	// Небольшое кол-во - стандартное
	case totalFiles < 1000:
		return cpuCount

	// Среднее кол-во - CPU + I/O bound
	case totalFiles < 100000:
		return cpuCount * 2

	// Большое кол-во - I/O bound, нужно много воркеров
	case totalFiles < 1000000:
		return cpuCount * 4

	// Огромное кол-во - максимум воркеров при ограничении памяти
	default:
		maxWorkers := cpuCount * 8
		if maxWorkers > 256 {
			maxWorkers = 256 // Мягкое ограничение
		}
		return maxWorkers
	}
}

// calculateFullHashWorkerCount выбирает количество воркеров для этапа полного хеширования.
// Полный хеш более CPU-heavy, поэтому для малых партий не опускаемся до 1 воркера.
func calculateFullHashWorkerCount(totalFiles int64) int {
	if totalFiles <= 0 {
		return 1
	}

	cpuCount := runtime.NumCPU()
	maxWorkers := cpuCount * 4
	if maxWorkers > 256 {
		maxWorkers = 256
	}

	workers := cpuCount
	if workers < 2 && totalFiles > 1 {
		workers = 2
	}

	if totalFiles < int64(workers) {
		workers = int(totalFiles)
		if workers < 1 {
			workers = 1
		}
	}

	if workers > maxWorkers {
		workers = maxWorkers
	}

	return workers
}

// calculateQuickHashBlockSize вычисляет оптимальный размер блока для быстрого хеша
// Адаптируется к размеру файла для баланса между скоростью и точностью
func calculateQuickHashBlockSize(fileSize int64) int64 {
	const minBlockSize = 1 * 1024             // 1 KB - минимум
	const maxBlockSize = 10 * 1024 * 1024     // 10 MB - максимум
	const maxMemoryPerFile = 20 * 1024 * 1024 // 20 MB на файл

	var blockSize int64

	switch {
	// Для очень маленьких файлов - весь файл
	case fileSize < 100*1024:
		blockSize = fileSize

	// Для маленьких файлов (100KB - 1MB) - 10%
	case fileSize < 1024*1024:
		blockSize = fileSize / 10

	// Для средних файлов (1MB - 100MB) - 5%
	case fileSize < 100*1024*1024:
		blockSize = fileSize / 20

	// Для больших файлов (100MB - 1GB) - 2%
	case fileSize < 1024*1024*1024:
		blockSize = fileSize / 50

	// Для очень больших файлов (> 1GB) - 1%
	case fileSize < 10*1024*1024*1024:
		blockSize = fileSize / 100

	// Для гигантских файлов (> 10GB) - ограничиваем память
	default:
		blockSize = maxMemoryPerFile / 2 // 10MB на блок
	}

	// Применяем минимум и максимум
	if blockSize < minBlockSize {
		blockSize = minBlockSize
	}
	if blockSize > maxBlockSize {
		blockSize = maxBlockSize
	}

	return blockSize
}

// quickXXH3File вычисляет быстрый хеш файла (адаптивный размер)
// Использует calculateQuickHashBlockSize для оптимизации в зависимости от размера файла
func quickXXH3File(filePath string, fileSize int64) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := xxh3.New()

	// Для маленьких файлов (< 1KB) хеш всего файла
	if fileSize < 1024 {
		if _, err := io.Copy(hasher, file); err != nil {
			return "", err
		}
		return fmt.Sprintf("%x", hasher.Sum(nil)), nil
	}

	// Вычисляем оптимальный размер блока
	blockSize := calculateQuickHashBlockSize(fileSize)

	// Читаем начало
	startBuf := make([]byte, blockSize)
	n, err := file.Read(startBuf)
	if err != nil && err != io.EOF {
		return "", err
	}
	hasher.Write(startBuf[:n])

	// Для маленьких файлов (< 2 блока) - только начало
	if fileSize <= blockSize*2 {
		return fmt.Sprintf("%x", hasher.Sum(nil)), nil
	}

	// Для больших файлов - также конец
	endBuf := make([]byte, blockSize)
	file.Seek(-blockSize, io.SeekEnd)
	n, err = file.Read(endBuf)
	if err != nil && err != io.EOF {
		return "", err
	}
	hasher.Write(endBuf[:n])

	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}

// fullXXH3File вычисляет полный XXH3 хеш файла
func fullXXH3File(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := xxh3.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hasher.Sum(nil)), nil
}

// fileExists проверяет существование файла
func fileExists(filepath string) bool {
	_, err := os.Stat(filepath)
	return err == nil
}

// dirExists проверяет существование директории
func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

// isProcessAlive проверяет, живой ли процесс по PID
func isProcessAlive(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}

	// Signal(0) проверяет существование процесса без отправки реального сигнала.
	err = process.Signal(syscall.Signal(0))
	if err == nil {
		return true
	}

	errStr := strings.ToLower(err.Error())
	if strings.Contains(errStr, "no such process") || strings.Contains(errStr, "process already finished") || strings.Contains(errStr, "not found") {
		return false
	}

	// Для остальных ошибок (например, нехватка прав) считаем процесс существующим.
	return true
}

// acquireLock пытается получить эксклюзивную блокировку для предотвращения одновременного запуска
// использует флаг os.O_EXCL для гарантии, что только один процесс может захватить блокировку
func acquireLock(lockFile string) (*os.File, error) {
	// Сначала проверим, существует ли уже lockfile и живой ли процесс, который его создал
	if data, err := os.ReadFile(lockFile); err == nil {
		// Файл существует, пытаемся прочитать PID
		pidStr := strings.TrimSpace(string(data))
		if pid, err := strconv.Atoi(pidStr); err == nil {
			// Если процесс жив, блокировка активна - отказываем
			if isProcessAlive(pid) {
				return nil, fmt.Errorf("другой экземпляр уже запущен (PID: %d)", pid)
			}
			// Процесс мертв - удаляем мертвый lockfile
			os.Remove(lockFile)
		}
	}

	// Попытаемся создать файл с флагом эксклюзивного доступа
	// Если файл уже существует, функция вернёт ошибку
	file, err := os.OpenFile(lockFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		if os.IsExist(err) {
			// Файл уже существует - другой экземпляр запущен
			return nil, fmt.Errorf("другой экземпляр уже запущен")
		}
		// Другая ошибка при открытии
		return nil, err
	}

	// Записываем PID текущего процесса в файл для отладки
	fmt.Fprintf(file, "%d", os.Getpid())

	return file, nil
}

// releaseLock освобождает блокировку и удаляет lock файл
func releaseLock(lockFile string, lockHandle *os.File) error {
	if lockHandle != nil {
		lockHandle.Close()
		// Удаляем файл блокировки
		os.Remove(lockFile)
	}
	return nil
}

// removeEmptyDirectories рекурсивно удаляет пустые каталоги в указанной директории
func removeEmptyDirectories(root string) int {
	removedCount := 0

	// Используем filepath.Walk для обхода всех файлов
	// но нам нужно обходить снизу вверх (post-order), чтобы удалялись сначала глубокие пустые папки
	// Поэтому сначала создаем список всех директорий
	var dirs []string

	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() && path != root {
			dirs = append(dirs, path)
		}
		return nil
	})

	// Сортируем директории так, чтобы более глубокие были в конце
	// (это предпочтительный порядок для удаления - снизу вверх)
	// В Go нет встроенной сортировки по глубине, но мы можем отсортировать по длине пути
	// Директории с большей длиной пути будут удаляться первыми
	sort.Slice(dirs, func(i, j int) bool {
		return len(dirs[i]) > len(dirs[j]) // Обратный порядок (более глубокие первыми)
	})

	// Пытаемся удалить каждую директорию
	for _, dir := range dirs {
		// Проверяем, пуста ли директория
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue // Пропускаем если не можем прочитать
		}

		if len(entries) == 0 {
			// Директория пуста, пытаемся удалить
			if err := os.Remove(dir); err == nil {
				removedCount++
				if testMode {
					logPrintf("Пустой каталог будет удален: %s\n", dir)
				} else {
					logPrintf("Пустой каталог удален: %s\n", dir)
				}
			}
		}
	}

	return removedCount
}
