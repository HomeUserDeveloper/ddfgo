package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

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

// TestPerformanceAnalysis - не тест, а целевая функция для профилирования
func TestPerformanceAnalysis(t *testing.T) {
	fmt.Println("Рекомендуемые команды для анализа производительности:")
	fmt.Println("1. Запуск всех бенчмарков:")
	fmt.Println("   go test -bench=. -benchmem -cpuprofile=cpu.prof -memprofile=mem.prof")
	fmt.Println("2. Анализ результатов:")
	fmt.Println("   go tool pprof -http=:8080 ddfgo.test cpu.prof")
	fmt.Println("   go tool pprof -http=:8080 ddfgo.test mem.prof")
}
