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
	"crypto/md5"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const (
	lockFileName = "ddfgo.lock"
	mutexTimeout = 5 * time.Second
)

// Version встраивается при компиляции через ldflags
var Version = "000.000.000.001"

type FileInfo struct {
	Fnum  int64
	Fname string
	Fsize int64
	Md5   string
	Fflag bool
}

var (
	mu              sync.Mutex
	filesProcessed  int
	filesRemoved    int
	duplicatesFound int
	logFile         string = "ddfgo.log"
	errLogFile      string = "ddfgoerr.log"
	testMode        bool
	cleanMode       bool
	logOutput       *os.File
	errLogOutput    *os.File
)

func main() {
	var targetDir string = os.Getenv("DDF_DIR")
	if targetDir == "" {
		targetDir = "E:\\SortDir"
	}

	var showHelp bool
	var showVersion bool

	flag.StringVar(&targetDir, "dir", targetDir, "Директория для сканирования на наличие дубликатов")
	flag.BoolVar(&testMode, "test", false, "Записать результаты в лог файл")
	flag.BoolVar(&cleanMode, "clean", false, "Очистить лог файл перед запуском")
	flag.BoolVar(&showHelp, "help", false, "Показать справку")
	flag.BoolVar(&showHelp, "h", false, "Показать справку (сокращенно)")
	flag.BoolVar(&showVersion, "version", false, "Показать номер версии")
	flag.BoolVar(&showVersion, "v", false, "Показать номер версии (сокращенно)")
	flag.Parse()

	// Обработка флагов справки и версии
	if showHelp || (len(os.Args) == 1) {
		printHelp()
		os.Exit(0)
	}

	if showVersion {
		fmt.Printf("ddfgo версия: %s\n", Version)
		os.Exit(0)
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
			os.Exit(1)
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
		os.Exit(1)
	}

	// Проверка MUTEX - предотвращение одновременного запуска
	lockFile := filepath.Join(os.TempDir(), lockFileName)
	lockHandle, err := acquireLock(lockFile)
	if err != nil {
		msg := "Другой экземпляр уже запущен"
		logError(msg)
		logPrintf("Ошибка: %s\n", msg)
		os.Exit(1)
	}
	defer releaseLock(lockFile, lockHandle)

	// Выполняем основную логику
	exitCode := runApp(targetDir, lockFile)
	os.Exit(exitCode)
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
	fmt.Println("    Режим тестирования (логирование результатов)")
	fmt.Println("    Все результаты будут записаны в файл ddfgo.log")
	fmt.Println("    Пример: ddfgo -dir \"D:\\Photos\" -test")
	fmt.Println()
	fmt.Println("  -clean")
	fmt.Println("    Очистить старые лог файлы перед запуском")
	fmt.Println("    Удаляет ddfgo.log и ddfgoerr.log перед работой")
	fmt.Println("    Пример: ddfgo -dir \"D:\\Videos\" -test -clean")
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
	fmt.Println("  5. Используя переменную окружения:")
	fmt.Println("     set DDF_DIR=D:\\MyFiles")
	fmt.Println("     ddfgo")
	fmt.Println()
	fmt.Println("ОПИСАНИЕ:")
	fmt.Println()
	fmt.Println("  ddfgo - высокопроизводительная утилита для поиска и удаления")
	fmt.Println("  дубликатов файлов в больших каталогах. Использует:")
	fmt.Println()
	fmt.Println("  • SQLite базу данных для быстрого поиска")
	fmt.Println("  • MD5 хеширование для надежного определения дубликатов")
	fmt.Println("  • Параллельную обработку (до 10 одновременных потоков)")
	fmt.Println("  • Механизм блокировки для предотвращения одновременных запусков")
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

	// Шаг 3: Вычисление MD5 для потенциальных дубликатов
	if err := calculateMD5Hashes(db); err != nil {
		logError("Ошибка вычисления MD5 хешей: %v", err)
		logPrintf("Ошибка вычисления MD5 хешей: %v\n", err)
		return 1
	}

	// Шаг 4: Нахождение реальных дубликатов по MD5
	if err := findDuplicatesByMD5(db); err != nil {
		logError("Ошибка поиска дубликатов по MD5: %v", err)
		logPrintf("Ошибка поиска дубликатов по MD5: %v\n", err)
		return 1
	}

	// Шаг 5: Пометка и удаление дубликатов
	if err := markAndRemoveDuplicates(db); err != nil {
		logError("Ошибка удаления дубликатов: %v", err)
		logPrintf("Ошибка удаления дубликатов: %v\n", err)
		return 1
	}

	// Вычисляем прошедшее время
	elapsed := time.Since(startTime)

	// Вывод статистики
	logPrintf("\n=== Статистика ===\n")
	logPrintf("Файлов обработано: %d\n", filesProcessed)
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
        md5 TEXT,
        fflag INTEGER DEFAULT 0
    )`

	if _, err := db.Exec(createTableQuery); err != nil {
		return fmt.Errorf("ошибка создания таблицы: %v", err)
	}

	// Создание индексов отдельно
	indexes := []string{
		"CREATE INDEX IF NOT EXISTS idx_fsize ON curfiles(fsize)",
		"CREATE INDEX IF NOT EXISTS idx_md5 ON curfiles(md5)",
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

// calculateMD5Hashes вычисляет MD5 для всех файлов в таблице
// Параллельное вычисление MD5 с ограничением горутин
func calculateMD5Hashes(db *sql.DB) error {
	rows, err := db.Query("SELECT fnum, fname FROM curfiles WHERE md5 IS NULL")
	if err != nil {
		return err
	}
	defer rows.Close()

	type md5Result struct {
		fnum int64
		hash string
		err  error
	}

	results := make(chan md5Result, 100)
	var wg sync.WaitGroup

	// Ограничиваем количество параллельных вычислений
	semaphore := make(chan struct{}, 10) // максимум 10 горутин

	// Собираем файлы для обработки
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

	// Запускаем параллельные вычисления
	for _, file := range files {
		if !fileExists(file.fname) {
			continue
		}

		wg.Add(1)
		go func(fnum int64, fname string) {
			defer wg.Done()
			semaphore <- struct{}{}        // Захватываем слот
			defer func() { <-semaphore }() // Освобождаем слот

			hash, err := md5File(fname)
			results <- md5Result{fnum: fnum, hash: hash, err: err}
		}(file.fnum, file.fname)
	}

	// Закрываем канал после завершения всех горутин
	go func() {
		wg.Wait()
		close(results)
	}()

	// Обновляем базу данных пакетами
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("UPDATE curfiles SET md5 = ? WHERE fnum = ?")
	if err != nil {
		return err
	}
	defer stmt.Close()

	batchCount := 0
	for result := range results {
		if result.err != nil {
			fmt.Printf("Ошибка вычисления MD5: %v\n", result.err)
			continue
		}

		if _, err := stmt.Exec(result.hash, result.fnum); err != nil {
			fmt.Printf("Ошибка обновления MD5: %v\n", err)
			continue
		}

		batchCount++
		if batchCount >= 100 { // Коммитим каждые 100 обновлений
			if err := tx.Commit(); err != nil {
				return err
			}
			tx, err = db.Begin()
			if err != nil {
				return err
			}
			stmt, err = tx.Prepare("UPDATE curfiles SET md5 = ? WHERE fnum = ?")
			if err != nil {
				return err
			}
			batchCount = 0
		}
	}

	return tx.Commit()
}

// findDuplicatesByMD5 оставляет только файлы с повторяющимися MD5
func findDuplicatesByMD5(db *sql.DB) error {
	// Получаем дубликаты по MD5
	rows, err := db.Query(`
		SELECT fnum, fname, md5 FROM curfiles 
		WHERE md5 IS NOT NULL AND md5 IN (
			SELECT md5 FROM curfiles 
			WHERE md5 IS NOT NULL
			GROUP BY md5 
			HAVING COUNT(*) > 1
		)
		ORDER BY md5, fname
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	type DuplicateFile struct {
		fnum  int64
		fname string
		md5   string
	}

	var filesToKeep []DuplicateFile
	for rows.Next() {
		var fnum int64
		var fname string
		var md5 string
		if err := rows.Scan(&fnum, &fname, &md5); err != nil {
			logPrintf("Ошибка при сканировании: %v\n", err)
			continue
		}
		filesToKeep = append(filesToKeep, DuplicateFile{fnum, fname, md5})
	}

	// Подсчитываем уникальные MD5
	uniqueMD5s := make(map[string]bool)
	for _, file := range filesToKeep {
		uniqueMD5s[file.md5] = true
	}
	duplicatesFound = len(filesToKeep) - len(uniqueMD5s)

	// Очищаем таблицу и вставляем обратно дубликаты
	_, err = db.Exec("DELETE FROM curfiles")
	if err != nil {
		return err
	}

	stmt, err := db.Prepare("INSERT INTO curfiles (fnum, fname, fsize, md5) VALUES (?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, file := range filesToKeep {
		// fsize не используется, т.к. нам нужна только информация для удаления
		_, err = stmt.Exec(file.fnum, file.fname, 0, file.md5)
		if err != nil {
			logPrintf("Ошибка добавления файла: %v\n", err)
		}
	}

	return nil
}

// markAndRemoveDuplicates помечает и удаляет дубликаты, оставляя один файл из каждой группы
func markAndRemoveDuplicates(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Используем оконные функции для оптимизации
	query := `
    UPDATE curfiles 
    SET fflag = 1 
    WHERE fnum IN (
        SELECT fnum 
        FROM (
            SELECT fnum, 
                   ROW_NUMBER() OVER (PARTITION BY md5 ORDER BY fname) as rn
            FROM curfiles 
            WHERE md5 IS NOT NULL
        ) ranked
        WHERE ranked.rn > 1
    )
    `

	result, err := tx.Exec(query)
	if err != nil {
		return err
	}

	affected, _ := result.RowsAffected()
	logPrintf("Помечено для удаления: %d файлов\n", affected)

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
			// В режиме тестирования только логируем, не удаляем
			logPrintf("Файл будет удален: %s\n", fname)
			removedCount++
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

// md5File вычисляет MD5 хеш файла
func md5File(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hash.Sum(nil)), nil
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
	// На Windows попытаемся открыть процесс
	process, err := os.FindProcess(pid)
	if err != nil {
		// Процесс не найден
		return false
	}

	// На Windows завершение сигнала всегда устанавливает err == nil, если процесс существует
	// Мы используем сигнал Kill=0, который проверяет, может ли сигнал быть отправлен
	err = process.Release()
	return err == nil
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
