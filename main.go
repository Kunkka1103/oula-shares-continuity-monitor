package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

var (
	opsDSN   = flag.String("opsDsn", "", "MySQL DSN, e.g. user:password@tcp(host:3306)/ops_db")
	pushAddr = flag.String("push-url", "http://localhost:9091", "Prometheus Pushgateway URL")
	interval = flag.Int("interval", 5, "Check interval in minutes")
)

func main() {
	flag.Parse()
	if *opsDSN == "" || *pushAddr == "" {
		log.Panicln("Both opsDsn and push-url parameters are required.")
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
			log.Println("Error getting share counts:", err)
			time.Sleep(time.Minute * time.Duration(*interval))
			continue
		}

		// 推送每个链的最大非0高度
		for chain, epoch := range maxEpochs {
			// 推送指标，直接使用链名作为 job 标签
			err = pushMaxEpoch(*pushAddr, chain, epoch)
			if err != nil {
				log.Printf("Error pushing max epoch for %s: %v", chain, err)
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

// 推送当前链的最新分享计数到 Prometheus Pushgateway
func pushMaxEpoch(pushAddr, chain string, maxEpoch int64) error {
	// 创建指标
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: fmt.Sprintf("%s_max_epoch_nonzero", chain), // 使用链名作为指标名
		Help: fmt.Sprintf("max epoch nozero for chain %s", chain),
	})

	// 设置指标值
	gauge.Set(float64(maxEpoch))

	// 推送指标
	err := push.New(pushAddr, chain).Collector(gauge).Push()
	if err != nil {
		return err
	}

	log.Printf("Pushed %s_shares_count{job=\"%s\"} = %d", chain, chain, maxEpoch)
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
