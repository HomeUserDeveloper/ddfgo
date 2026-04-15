package main

import (
	"fmt"
	"os"
	"path/filepath"
)

func init() {
	// Это init функция в точной копии ddfgo.go, поэтому не будет конфликта main()
	// Давайте создадим отдельный файл с функцией которая не конфликтует
}

func setupTestDataWithLargeFiles() {
	testDir := "test_sf_check"

	// Create large file content
	largeContent := ""
	for i := 0; i < 300; i++ {
		largeContent += fmt.Sprintf("This is line %d with some content to make file bigger. \n", i)
	}

	// Write large1.txt
	os.WriteFile(filepath.Join(testDir, "large1.txt"), []byte(largeContent), 0644)
	os.WriteFile(filepath.Join(testDir, "large2.txt"), []byte(largeContent), 0644)

	// Check file sizes
	fi1, _ := os.Stat(filepath.Join(testDir, "large1.txt"))
	fmt.Printf("Updated file sizes:\n")
	fmt.Printf("  large1.txt: %d bytes (%.2f KB)\n", fi1.Size(), float64(fi1.Size())/1024)
	fmt.Printf("  large2.txt: %d bytes (%.2f KB)\n", fi1.Size(), float64(fi1.Size())/1024)
}
