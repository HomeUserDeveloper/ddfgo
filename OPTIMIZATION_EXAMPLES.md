================================================================================
                ПРИМЕРЫ КОДА ДЛЯ ОПТИМИЗАЦИЙ ddfgo
================================================================================

В этом файле приведены конкретные примеры кода для реализации рекомендаций
по оптимизации, выявленных в результате профилирования.

================================================================================
ПРИОРИТЕТ 1: ОПТИМИЗАЦИЯ СКАНИРОВАНИЯ ДИРЕКТОРИИ
================================================================================

ПРОБЛЕМА:
- filepath.Walk создает много объектов на heap
- ~13,800 аллокаций на каждые 1,000 файлов
- Это замедляет сборщик мусора

РЕШЕНИЕ 1: Переиспользование буферов в структуре
────────────────────────────────────────────────────────────────────────────

// До оптимизации (текущий код):
func scanDirectory(root string, db *sql.DB) error {
    tx, err := db.Begin()
    // ... создается много временных структур в filepath.Walk
}

// После оптимизации:
type DirectoryScannerOptimized struct {
    db              *sql.DB
    tx              *sql.Tx
    stmt            *sql.Stmt
    batchBuf        []struct {
        path string
        size int64
    }
    batchCapacity   int
    batchCount      int
    filesProcessed  int
}

func (s *DirectoryScannerOptimized) Scan(root string) error {
    s.batchBuf = make([]struct {
        path string
        size int64
    }, s.batchCapacity)
    
    s.batchCount = 0
    s.filesProcessed = 0
    
    // Используем преалокированные буферы вместо создания новых
    return filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
        if err != nil || info.IsDir() {
            return nil
        }
        
        // Проверяем расширение
        if !extensionAll {
            ext := strings.ToLower(filepath.Ext(path))
            if expandToSkip[ext] {
                return nil
            }
        }
        
        // Добавляем в буфер
        s.batchBuf[s.batchCount].path = path
        s.batchBuf[s.batchCount].size = info.Size()
        s.batchCount++
        s.filesProcessed++
        
        // Коммитим пакет когда буфер полон
        if s.batchCount >= s.batchCapacity {
            if err := s.flushBatch(); err != nil {
                return err
            }
        }
        
        return nil
    })
}

func (s *DirectoryScannerOptimized) flushBatch() error {
    if s.batchCount == 0 {
        return nil
    }
    
    for i := 0; i < s.batchCount; i++ {
        if _, err := s.stmt.Exec(
            s.batchBuf[i].path,
            s.batchBuf[i].size,
        ); err != nil {
            return err
        }
    }
    
    if err := s.tx.Commit(); err != nil {
        return err
    }
    
    // Новая транзакция для следующего пакета
    var err error
    s.tx, err = s.db.Begin()
    if err != nil {
        return err
    }
    
    s.stmt = s.tx.Prepare("INSERT OR IGNORE INTO curfiles (fname, fsize) VALUES (?, ?)")
    s.batchCount = 0
    
    return nil
}

ОЖИДАЕМЫЙ РЕЗУЛЬТАТ:
- Уменьшение аллокаций: 20-30%
- Ускорение в целом: 2-5%
- Сокращение GC pauses


================================================================================
ПРИОРИТЕТ 1: АДАПТИВНЫЙ РАЗМЕР БЫСТРОГО ХЕША
================================================================================

ПРОБЛЕМА:
- Фиксированное значение "5%" может быть недостаточно для больших файлов
- Для файлов 100+ MB риск ложных положительных результатов

ТЕКУЩИЙ КОД (quickXXH3File):
────────────────────────────────────────────────────────────────────────────
func quickXXH3File(filePath string, fileSize int64) (string, error) {
    // ...
    blockSize := int64(float64(fileSize) * 0.05)  // <- Фиксированное 5%
    if blockSize < 1 {
        blockSize = 1
    }
    // ...
}

ОПТИМИЗИРОВАННЫЙ КОД:
────────────────────────────────────────────────────────────────────────────
// calculateQuickHashBlockSize вычисляет оптимальный размер блока для хеша
func calculateQuickHashBlockSize(fileSize int64) int64 {
    const minBlockSize = 1 * 1024               // 1 KB - минимум
    const maxBlockSize = 10 * 1024 * 1024       // 10 MB - максимум
    const maxMemoryPerFile = 20 * 1024 * 1024   // 20 MB на файл
    
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
        blockSize = maxMemoryPerFile / 2  // 10MB на блок
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

// Обновленная версия quickXXH3File:
func quickXXH3FileOptimized(filePath string, fileSize int64) (string, error) {
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

    // Для маленьких файлов - только начало
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

ОЖИДАЕМЫЙ РЕЗУЛЬТАТ:
- Лучшая точность определения дубликатов
- Уменьшение ложных положительных результатов
- Адаптивность к размеру файла


================================================================================
ПРИОРИТЕТ 2: АДАПТИВНОЕ УПРАВЛЕНИЕ ГОРУТИНАМИ
================================================================================

ТЕКУЩИЙ КОД:
────────────────────────────────────────────────────────────────────────────
// calculateQuickHashes использует фиксированное значение
numWorkers := runtime.NumCPU() * 2
semaphore := make(chan struct{}, numWorkers)

ОПТИМИЗИРОВАННЫЙ КОД:
────────────────────────────────────────────────────────────────────────────
// calculateOptimalWorkerCount определяет оптимальное кол-во воркеров
func calculateOptimalWorkerCount(totalFiles int64) int {
    cpuCount := runtime.NumCPU()
    
    // Для больших количеств файлов используем разные стратегии
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
            maxWorkers = 256  // Мягкое ограничение
        }
        return maxWorkers
    }
}

// Использование:
func calculateHashesOptimized(db *sql.DB, totalFiles int64) error {
    rows, _ := db.Query("SELECT COUNT(*) FROM curfiles WHERE quick_hash IS NULL")
    numWorkers := calculateOptimalWorkerCount(totalFiles)
    
    logPrintf("Использование %d воркеров для%d файлов\n", numWorkers, totalFiles)
    
    semaphore := make(chan struct{}, numWorkers)
    // ... остальной код
}

ОЖИДАЕМЫЙ РЕЗУЛЬТАТ:
- Лучшее использование ресурсов процессора и I/O
- Ускорение 5-15% в зависимости от размера данных
- Адаптивность к количеству файлов


================================================================================
ПРИОРИТЕТ 2: ПОТОКОВАЯ ОБРАБОТКА ДУБЛИКАТОВ
================================================================================

ПРОБЛЕМА:
- findDuplicatesByHash читает ВСЕ дубликаты в память
- На 100K дубликатов может потребоваться большой буфер
- Не масштабируется на миллионы файлов

ТЕКУЩИЙ КОД:
────────────────────────────────────────────────────────────────────────────
func findDuplicatesByHash(db *sql.DB) error {
    // Получаем ВСЕ дубликаты одним запросом
    rows, _ := db.Query("SELECT ... WHERE full_hash IN (SELECT ...)")
    
    var filesToKeep []DuplicateFile
    for rows.Next() {
        // Читаем ВСЕ в память
    }
    // Обрабатываем весь массив
}

ОПТИМИЗИРОВАННЫЙ КОД:
────────────────────────────────────────────────────────────────────────────
func findDuplicatesByHashStreaming(db *sql.DB) error {
    const batchSize = 1000
    
    // Шаг 1: Получаем список всех уникальных hash'ей найденные дубликатов
    hashRows, err := db.Query(`
        SELECT full_hash 
        FROM curfiles 
        WHERE full_hash IS NOT NULL
        GROUP BY full_hash 
        HAVING COUNT(*) > 1
    `)
    if err != nil {
        return err
    }
    defer hashRows.Close()
    
    // Обрабатываем хеши пакетами
    var processedCount int64
    
    for hashRows.Next() {
        var fullHash string
        if err := hashRows.Scan(&fullHash); err != nil {
            continue
        }
        
        // Обрабатываем дубликаты для одного хеша
        if err := processDuplicateGroup(db, fullHash); err != nil {
            logPrintf("Ошибка обработки группы дубликатов: %v\n", err)
        }
        
        processedCount++
        if processedCount%100 == 0 {
            logPrintf("Обработано групп дубликатов: %d\n", processedCount)
        }
    }
    
    return nil
}

func processDuplicateGroup(db *sql.DB, fullHash string) error {
    // Получаем все дубликаты для одного хеша
    rows, err := db.Query(`
        SELECT fnum, fname, fsize 
        FROM curfiles 
        WHERE full_hash = ? 
        ORDER BY fname
    `, fullHash)
    if err != nil {
        return err
    }
    defer rows.Close()
    
    type FileInfo struct {
        fnum  int64
        fname string
        fsize int64
    }
    
    var files []FileInfo
    for rows.Next() {
        var fnum int64
        var fname string
        var fsize int64
        
        if err := rows.Scan(&fnum, &fname, &fsize); err != nil {
            continue
        }
        
        files = append(files, FileInfo{fnum, fname, fsize})
    }
    
    // Помечаем для удаления (оставляем первый)
    if len(files) > 1 {
        stmt, _ := db.Prepare("UPDATE curfiles SET fflag = 1 WHERE fnum = ?")
        defer stmt.Close()
        
        for _, file := range files[1:] {
            stmt.Exec(file.fnum)
        }
    }
    
    return nil
}

ОЖИДАЕМЫЙ РЕЗУЛЬТАТ:
- Константное использование памяти (не зависит от кол-ва дубликатов)
- Масштабируемость на миллионы файлов
- Ускорение на больших объемах


================================================================================
ПРИОРИТЕТ 3: СПЕЦИАЛИЗИРОВАННАЯ ИДЕНТИФИКАЦИЯ ФАЙЛОВ
================================================================================

НОВАЯ ФУНКЦИЯ:
────────────────────────────────────────────────────────────────────────────
// FileTypeSignature хранит информацию о типе файла по его заголовку
type FileTypeSignature struct {
    name     string
    offsets  []int64 // Где искать сигнатуру
    patterns [][]byte // Сами сигнатуры
    skipFull bool  // Пропустить полное хеширование
}

var fileTypeSignatures = []FileTypeSignature{
    {
        name: "MP4",
        offsets: []int64{4},
        patterns: [][]byte{[]byte("ftyp")},
        skipFull: false,  // MP4 может быть дубликатом видео
    },
    {
        name: "ZIP",
        offsets: []int64{0},
        patterns: [][]byte{[]byte{0x50, 0x4B, 0x03, 0x04}},
        skipFull: false,
    },
    {
        name: "RAR",
        offsets: []int64{0},
        patterns: [][]byte{[]byte("Rar!\x1A\x07")},
        skipFull: false,
    },
}

func identifyFileType(filePath string) (string, bool, error) {
    file, err := os.Open(filePath)
    if err != nil {
        return "", false, err
    }
    defer file.Close()
    
    // Читаем только первые 512 байт
    header := make([]byte, 512)
    n, err := file.Read(header)
    if err != nil && err != io.EOF {
        return "", false, err
    }
    
    header = header[:n]
    
    // Проверяем против сигнатур
    for _, sig := range fileTypeSignatures {
        for _, pattern := range sig.patterns {
            for _, offset := range sig.offsets {
                if int64(len(header)) > offset+int64(len(pattern)) {
                    if bytes.Equal(header[offset:offset+int64(len(pattern))], pattern) {
                        return sig.name, sig.skipFull, nil
                    }
                }
            }
        }
    }
    
    return "UNKNOWN", false, nil
}

ИСПОЛЬЗОВАНИЕ:
────────────────────────────────────────────────────────────────────────────
func calculateHashesWithOptimization(db *sql.DB) error {
    // ... код для получения файлов ...
    
    for _, file := range files {
        // Быстрая идентификация типа
        fileType, canSkip, _ := identifyFileType(file.fname)
        
        quickHash, _ := quickXXH3File(file.fname, file.fsize)
        
        // Если это известный архив, пропускаем полное хеширование
        if canSkip && fileType == "ZIP" {
            // Используем размер + быстрый хеш вместо полного
            fullHash := quickHash + "_" + fileType
            updateDatabase(file.fnum, quickHash, fullHash)
        } else {
            // Обычный процесс
            fullHash, _ := fullXXH3File(file.fname)
            updateDatabase(file.fnum, quickHash, fullHash)
        }
    }
}

ОЖИДАЕМЫЙ РЕЗУЛЬТАТ:
- На архивах/видео: ускорение в 2-5 раз
- Улучшенная точность определения дубликатов
- Минимальное добавление кода


================================================================================
ЗАКЛЮЧЕНИЕ
================================================================================

Приоритет реализации:
1️⃣  Приоритет 1 (легко, 5-12% ускорение): Оптимизация сканирования
2️⃣  Приоритет 2 (средне, +15-20% дополнительно): Адаптивные параметры
3️⃣  Приоритет 3 (сложно, максимум 30%): Специализированные оптимизации

Начните с Приоритета 1 - это даст видимое улучшение при минимальном
риске введения ошибок. Затем оцените результаты и внедряйте дальше.
