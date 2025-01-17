package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	opsDSN    = flag.String("opsDsn", "", "MySQL DSN, e.g. user:password@tcp(host:3306)/ops_db")
	outputDir = flag.String("output-dir", "/opt/node-exporter/prom", "Directory to write Prometheus metric files")
	interval  = flag.Int("interval", 5, "Check interval in minutes")
)

func main() {
	flag.Parse()
	if *opsDSN == "" {
		log.Panicln("opsDsn is required.")
	}

	// 初始化 MySQL 连接
	db, err := initDB(*opsDSN)
	if err != nil {
		log.Panicln("Failed to open ops connection:", err)
	}
	defer db.Close()

	// 定期检查并推送数据
	for {
		// 获取每个链的最新高度和对应的最大非0高度
		maxEpochs, err := getMaxEpochs(db)
		if err != nil {
			log.Println("Error getting max epochs:", err)
			time.Sleep(time.Minute * time.Duration(*interval))
			continue
		}

		// 推送每个链的最大非0高度
		for chain, epoch := range maxEpochs {
			// 构建文件路径
			filePath := fmt.Sprintf("%s/%s_max_epoch_nozero.prom", *outputDir, chain)
			log.Printf("正在写入指标数据到 %s", filePath)

			// 使用封装好的函数写文件
			if err := writeToPromFile(filePath, chain, epoch); err != nil {
				log.Printf("写入文件 %s 时出错: %v", filePath, err)
			} else {
				log.Printf("成功写入到 %s", filePath)
			}
		}

		// 等待下次检查
		time.Sleep(time.Minute * time.Duration(*interval))
	}
}

// 初始化 MySQL 连接
func initDB(DSN string) (*sql.DB, error) {
	db, err := sql.Open("mysql", DSN)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	return db, nil
}

// 封装好的函数，用于写入 Prometheus 格式的数据到文件
func writeToPromFile(filePath, chain string, epochCount int64) error {
	file, err := os.OpenFile(filePath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("无法打开文件 %s: %v", filePath, err)
	}
	defer file.Close()

	_, err = fmt.Fprintf(
		file,
		"%s_shares_count{instance=\"jumperserver\",job=\"%s\"} %d\n",
		chain, chain, epochCount,
	)
	if err != nil {
		return fmt.Errorf("写入文件 %s 时发生错误: %v", filePath, err)
	}

	return nil
}

// 获取每个链的最大有效 epoch（share_count != 0）
func getMaxEpochs(db *sql.DB) (map[string]int64, error) {
	query := `
		SELECT chain, MAX(epoch) AS max_epoch
		FROM shares_epoch_counts
		WHERE share_count != 0
		GROUP BY chain
	`
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	maxEpochs := make(map[string]int64)

	for rows.Next() {
		var chain string
		var maxEpoch sql.NullInt64
		if err := rows.Scan(&chain, &maxEpoch); err != nil {
			return nil, err
		}
		if maxEpoch.Valid {
			maxEpochs[chain] = maxEpoch.Int64
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return maxEpochs, nil
}
