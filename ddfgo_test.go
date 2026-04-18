package main

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// BenchmarkQuickHash измеряет производительность быстрого хеширования
func BenchmarkQuickHash(b *testing.B) {
	// Создаем тестовые файлы
	testDir := filepath.Join(os.TempDir(), "bench_quick_hash")
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	// Создаем файлы разных размеров
	for i := 0; i < 10; i++ {
		content := make([]byte, 1024*1024) // 1MB файл
		for j := 0; j < len(content); j++ {
			content[j] = byte((i + j) * 7 % 256)
		}
		filePath := filepath.Join(testDir, fmt.Sprintf("test_%d.bin", i))
		os.WriteFile(filePath, content, 0644)
	}

	testFiles, _ := os.ReadDir(testDir)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, entry := range testFiles {
			if !entry.IsDir() {
				filePath := filepath.Join(testDir, entry.Name())
				info, _ := entry.Info()
				quickXXH3File(filePath, info.Size())
			}
		}
	}
}

// BenchmarkFullHash измеряет производительность полного хеширования
func BenchmarkFullHash(b *testing.B) {
	testDir := filepath.Join(os.TempDir(), "bench_full_hash")
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	// Создаем файлы
	for i := 0; i < 10; i++ {
		content := make([]byte, 1024*1024) // 1MB файл
		for j := 0; j < len(content); j++ {
			content[j] = byte((i + j) * 7 % 256)
		}
		filePath := filepath.Join(testDir, fmt.Sprintf("test_%d.bin", i))
		os.WriteFile(filePath, content, 0644)
	}

	testFiles, _ := os.ReadDir(testDir)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for _, entry := range testFiles {
			if !entry.IsDir() {
				filePath := filepath.Join(testDir, entry.Name())
				fullXXH3File(filePath)
			}
		}
	}
}

// BenchmarkDatabaseOperations измеряет производительность БД операций
func BenchmarkDatabaseOperations(b *testing.B) {
	dbFile := filepath.Join(os.TempDir(), "bench.db")
	defer os.Remove(dbFile)

	for i := 0; i < b.N; i++ {
		db, _ := sql.Open("sqlite", dbFile)
		initDatabase(db)

		// Вставка данных
		stmt, _ := db.Prepare("INSERT INTO curfiles (fname, fsize) VALUES (?, ?)")
		for j := 0; j < 1000; j++ {
			stmt.Exec(fmt.Sprintf("/path/to/file_%d.txt", j), int64(j*1024))
		}
		stmt.Close()

		db.Close()
		os.Remove(dbFile)
	}
}

// BenchmarkScanDirectory измеряет производительность сканирования директории
func BenchmarkScanDirectory(b *testing.B) {
	testDir := filepath.Join(os.TempDir(), "bench_scan")
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	// Создаем тестовые файлы
	for i := 0; i < 100; i++ {
		for j := 0; j < 10; j++ {
			subDir := filepath.Join(testDir, fmt.Sprintf("dir_%d", i))
			os.MkdirAll(subDir, 0755)
			content := []byte(fmt.Sprintf("test content %d-%d", i, j))
			os.WriteFile(filepath.Join(subDir, fmt.Sprintf("file_%d.txt", j)), content, 0644)
		}
	}

	dbFile := filepath.Join(os.TempDir(), "bench_scan.db")
	defer os.Remove(dbFile)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		db, _ := sql.Open("sqlite", dbFile)
		initDatabase(db)
		scanDirectory(testDir, db)
		db.Close()

		// Очищаем БД но оставляем файловую систему
		os.Remove(dbFile)
	}
}

// BenchmarkFindDuplicatesParallel измеряет параллельное вычисление хешей
func BenchmarkFindDuplicatesParallel(b *testing.B) {
	testDir := filepath.Join(os.TempDir(), "bench_parallel")
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	// Создаем файлы с дубликатами
	content := make([]byte, 100*1024) // 100KB
	for j := 0; j < len(content); j++ {
		content[j] = byte((j % 256))
	}

	for i := 0; i < 50; i++ {
		for j := 0; j < 20; j++ {
			filePath := filepath.Join(testDir, fmt.Sprintf("file_group%d_%d.bin", i, j))
			os.WriteFile(filePath, content, 0644)
		}
	}

	dbFile := filepath.Join(os.TempDir(), "bench_parallel.db")
	defer os.Remove(dbFile)

	db, _ := sql.Open("sqlite", dbFile)
	defer db.Close()

	initDatabase(db)
	scanDirectory(testDir, db)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		calculateHashes(db)
	}
}

// BenchmarkMemoryUsage измеряет использование памяти при различных операциях
func BenchmarkMemoryUsage(b *testing.B) {
	testDir := filepath.Join(os.TempDir(), "bench_memory")
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	dbFile := filepath.Join(os.TempDir(), "bench_memory.db")
	defer os.Remove(dbFile)

	db, _ := sql.Open("sqlite", dbFile)
	defer db.Close()

	initDatabase(db)

	// Создаем большое количество файлов в памяти
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 10000; j++ {
			stmt, _ := db.Prepare("INSERT INTO curfiles (fname, fsize) VALUES (?, ?)")
			stmt.Exec(fmt.Sprintf("/path/to/file_%d_%d.txt", i, j), int64(j*1024))
			stmt.Close()
		}
	}
}

// TestPerformanceAnalysis выводит актуальные команды для бенчмарков и pprof.
func TestPerformanceAnalysis(t *testing.T) {
	fmt.Println("Рекомендуемые команды для анализа производительности:")
	fmt.Println("1. Запуск всех бенчмарков:")
	fmt.Println("   go test -bench=. -benchmem")
	fmt.Println("2. Анализ результатов:")
	fmt.Println("   go test -c")
	fmt.Println("   go test -bench=. -benchmem -cpuprofile=cpu.prof -memprofile=mem.prof")
	fmt.Println("   go tool pprof -http=:8080 ddfgo.test cpu.prof")
	fmt.Println("   go tool pprof -http=:8080 ddfgo.test mem.prof")
}

func TestHelpDoesNotMentionRemovedProfileFlags(t *testing.T) {
	helpText := captureStdout(t, printHelp)

	if strings.Contains(helpText, "-cpuprofile") {
		t.Fatalf("help must not mention removed flag -cpuprofile")
	}
	if strings.Contains(helpText, "-memprofile") {
		t.Fatalf("help must not mention removed flag -memprofile")
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("create pipe: %v", err)
	}

	os.Stdout = w
	defer func() {
		os.Stdout = oldStdout
	}()

	fn()

	if err := w.Close(); err != nil {
		t.Fatalf("close write pipe: %v", err)
	}

	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read captured output: %v", err)
	}

	if err := r.Close(); err != nil {
		t.Fatalf("close read pipe: %v", err)
	}

	return string(out)
}

func TestCalculateQuickHashWorkerCount(t *testing.T) {
	tinyFiles := calculateQuickHashWorkerCount(5000, 5000*512*1024)
	largeFiles := calculateQuickHashWorkerCount(5000, 5000*2*1024*1024*1024)

	if tinyFiles <= largeFiles {
		t.Fatalf("expected more workers for tiny-file workload: tiny=%d large=%d", tinyFiles, largeFiles)
	}

	maxWorkers := runtime.NumCPU() * 8
	if maxWorkers > 256 {
		maxWorkers = 256
	}
	if tinyFiles < 1 || tinyFiles > maxWorkers {
		t.Fatalf("quick hash workers out of bounds: %d", tinyFiles)
	}
}

func TestCalculateFullHashWorkerCount(t *testing.T) {
	tinyFiles := calculateFullHashWorkerCount(5000, 5000*512*1024)
	largeFiles := calculateFullHashWorkerCount(5000, 5000*2*1024*1024*1024)

	if tinyFiles <= largeFiles {
		t.Fatalf("expected more workers for tiny-file full hash workload: tiny=%d large=%d", tinyFiles, largeFiles)
	}

	if got := calculateFullHashWorkerCount(0, 0); got != 1 {
		t.Fatalf("expected 1 worker for empty workload, got %d", got)
	}
}

func TestClampWorkerCount(t *testing.T) {
	if got := clampWorkerCount(64, 3, 1, 128); got != 3 {
		t.Fatalf("expected workers to be limited by file count, got %d", got)
	}
	if got := clampWorkerCount(0, 10, 1, 128); got != 1 {
		t.Fatalf("expected minimum worker count of 1, got %d", got)
	}
}

// TestKeepOldestDuplicateByMtime проверяет, что из группы дубликатов
// остаётся файл с наименьшим временем модификации (самый старый).
func TestKeepOldestDuplicateByMtime(t *testing.T) {
	dir := t.TempDir()

	content := []byte("identical content for dedup test 12345")

	// Создаём три файла с одинаковым содержимым.
	// Устанавливаем mtime вручную: oldest < middle < newest.
	type fileSpec struct {
		name  string
		mtime time.Time
	}
	now := time.Now()
	specs := []fileSpec{
		{"newest.txt", now},
		{"middle.txt", now.Add(-time.Hour)},
		{"oldest.txt", now.Add(-2 * time.Hour)},
	}

	for _, s := range specs {
		p := filepath.Join(dir, s.name)
		if err := os.WriteFile(p, content, 0644); err != nil {
			t.Fatalf("write %s: %v", s.name, err)
		}
		if err := os.Chtimes(p, s.mtime, s.mtime); err != nil {
			t.Fatalf("chtimes %s: %v", s.name, err)
		}
	}

	dbFile := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite", dbFile)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if err := initDatabase(db); err != nil {
		t.Fatalf("initDatabase: %v", err)
	}
	if err := scanDirectory(dir, db); err != nil {
		t.Fatalf("scanDirectory: %v", err)
	}
	if err := calculateHashes(db); err != nil {
		t.Fatalf("calculateHashes: %v", err)
	}
	if err := findDuplicatesByHash(db); err != nil {
		t.Fatalf("findDuplicatesByHash: %v", err)
	}

	// Устанавливаем removeAllFiles=true, чтобы размер файла не блокировал пометку.
	origAll := removeAllFiles
	removeAllFiles = true
	defer func() { removeAllFiles = origAll }()

	// Для этого теста проверяем режим сохранения самого старого файла.
	origKeepOld := keepOld
	keepOld = true
	defer func() { keepOld = origKeepOld }()

	if err := markAndRemoveDuplicates(db); err != nil {
		t.Fatalf("markAndRemoveDuplicates: %v", err)
	}

	// Проверяем: fflag=0 должен быть только у oldest.txt.
	rows, err := db.Query("SELECT fname, fflag FROM curfiles ORDER BY fname")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var fname string
		var fflag int
		if err := rows.Scan(&fname, &fflag); err != nil {
			t.Fatalf("scan: %v", err)
		}
		base := filepath.Base(fname)
		if base == "oldest.txt" && fflag != 0 {
			t.Errorf("oldest.txt должен быть сохранён (fflag=0), но fflag=%d", fflag)
		}
		if (base == "middle.txt" || base == "newest.txt") && fflag != 1 {
			t.Errorf("%s должен быть помечен к удалению (fflag=1), но fflag=%d", base, fflag)
		}
	}
}
