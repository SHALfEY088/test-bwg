package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	_ "github.com/jackc/pgx/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

var pgDB *sqlx.DB
var redisDB *redis.Client

// у приложения которое обрабатывает запросы на вывод проблема, оно допускает овердрафт.
// Необходимо модернизировать функцию вывода так, чтобы не допускать овердрафт и иметь
// возможность запускать приложение в нескольких репликах
// нельзя подключать сторонние библиотеки
// овердрафт - уход баланса клиента в минус
// баланс клиента - сумма всех вводов минус сумма всех вывводов

func main() {
	rand.Seed(time.Now().UnixNano())
	connectionURL := "host=127.0.0.1 port=5432 user=postgres password=postgres dbname=postgres"

	database, err := sqlx.Open("pgx", connectionURL)
	if err != nil {
		panic(err)
	}

	if err = database.Ping(); err != nil {
		panic(err)
	}
	pgDB = database
	//createTables()
	rds, err := NewRedisClient(Config{
		Host:               "127.0.0.1",
		Port:               "6379",
		MinIdleConns:       10,
		PoolSize:           10,
		PoolTimeout:        10,
		Password:           "dev",
		UseCertificates:    false,
		InsecureSkipVerify: false,
		CertificatesPaths: struct {
			Cert string
			Key  string
			Ca   string
		}{},
		DB: 1,
	})
	res := rds.FlushDB(context.Background())
	if res.Err() != nil {
		panic(res.Err())
	}
	redisDB = rds
	clear()

	for i := 0; i < 30; i++ {
		go createInvoice(randInRange(10, 50), randInRange(1, 3))
	}

	wg := new(sync.WaitGroup)
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			createWithdraw(randInRange(40, 100), randInRange(1, 3))
		}()
	}
	wg.Wait()

	fmt.Println(getBalance(1))
	fmt.Println(getBalance(2))
	fmt.Println(getBalance(3))
}

func getBalance(clientID int) (res int) {
	err := pgDB.Get(&res, `select (select coalesce(sum(amount), 0) from public.invoice where client_id=$1) - (select coalesce(sum(amount), 0) from public.withdraw  where client_id=$1)`, clientID)
	if err != nil {
		panic(err)
	}
	return res
}

func createInvoice(amount int, clientID int) {
	_, err := pgDB.Exec(`insert into public.invoice (amount, client_id) values ($1, $2);`, amount, clientID)
	if err != nil {
		panic(err)
	}
}

func createWithdraw(amount int, clientID int) {
	// Получение блокировки
	lockKey := fmt.Sprintf("withdraw_lock_%d", clientID)

	err := acquireLock(lockKey, 10*time.Second)
	if err != nil {
		fmt.Printf("Не удалось получить блокировку для клиента %d: %v\n", clientID, err)
		return
	}

	// Освобождение блокировки в конце
	defer func() {
		err := releaseLock(lockKey)
		if err != nil {
			fmt.Printf("Не удалось освободить блокировку для клиента %d: %v\n", clientID, err)
		}
	}()

	// Проверка, не приведет ли сумма вывода к овердрафту
	currentBalance := getBalance(clientID)
	if currentBalance < amount {
		fmt.Printf("Недостаточно средств для клиента %d: %d < %d\n", clientID, currentBalance, amount)
		return
	}

	_, err = pgDB.Exec(`insert into public.withdraw (amount, client_id) values ($1, $2);`, amount, clientID)
	if err != nil {
		panic(err)
	}
}

func acquireLock(lockKey string, lockTimeout time.Duration) error {
	maxRetries := 5
	retryDelay := 100 * time.Millisecond

	for i := 0; i < maxRetries; i++ {
		// Используем SETNX для установки ключа блокировки, если он не существует, с временем блокировки в качестве значения
		ok, err := redisDB.SetNX(context.Background(), lockKey, lockTimeout.Milliseconds(), lockTimeout).Result()
		if err != nil {
			return err
		}

		if ok {
			return nil
		}

		// Если не удалось получить блокировку, ожидаем некоторое время перед следующей попыткой
		time.Sleep(retryDelay)
	}

	return fmt.Errorf("не удалось получить блокировку после %d попыток: %s", maxRetries, lockKey)
}

func releaseLock(lockKey string) error {
	// Используем DEL для удаления ключа блокировки
	_, err := redisDB.Del(context.Background(), lockKey).Result()
	return err
}

func randInRange(min, max int) int {
	return int(float64(rand.Intn(max-min+1) + min))
}

func clear() {
	_, err := pgDB.Exec(`delete from public.withdraw;`)
	if err != nil {
		panic(err)
	}
	_, err = pgDB.Exec(`delete from public.invoice;`)
	if err != nil {
		panic(err)
	}
}

type Config struct {
	Host               string `validate:"required"`
	Port               string `validate:"required"`
	MinIdleConns       int    `validate:"required"`
	PoolSize           int    `validate:"required"`
	PoolTimeout        int    `validate:"required"`
	Password           string `validate:"required"`
	UseCertificates    bool
	InsecureSkipVerify bool
	CertificatesPaths  struct {
		Cert string
		Key  string
		Ca   string
	}
	DB int
}

func NewRedisClient(cfg Config) (*redis.Client, error) {
	opts := &redis.Options{}
	if cfg.UseCertificates {
		certs := make([]tls.Certificate, 0, 0)
		if cfg.CertificatesPaths.Cert != "" && cfg.CertificatesPaths.Key != "" {
			cert, err := tls.LoadX509KeyPair(cfg.CertificatesPaths.Cert, cfg.CertificatesPaths.Key)
			if err != nil {
				return nil, errors.Wrapf(
					err,
					"certPath: %v, keyPath: %v",
					cfg.CertificatesPaths.Cert,
					cfg.CertificatesPaths.Key,
				)
			}
			certs = append(certs, cert)
		}
		caCert, err := os.ReadFile(cfg.CertificatesPaths.Ca)
		if err != nil {
			return nil, errors.Wrapf(err, "ca load path: %v", cfg.CertificatesPaths.Ca)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		opts = &redis.Options{
			Addr:         fmt.Sprintf("%s:%s", cfg.Host, cfg.Port),
			MinIdleConns: cfg.MinIdleConns,
			PoolSize:     cfg.PoolSize,
			PoolTimeout:  time.Duration(cfg.PoolTimeout) * time.Second,
			Password:     cfg.Password,
			DB:           cfg.DB,
			TLSConfig: &tls.Config{
				InsecureSkipVerify: cfg.InsecureSkipVerify,
				Certificates:       certs,
				RootCAs:            caCertPool,
			},
		}
	} else {
		opts = &redis.Options{
			Addr:         fmt.Sprintf("%s:%s", cfg.Host, cfg.Port),
			MinIdleConns: cfg.MinIdleConns,
			PoolSize:     cfg.PoolSize,
			PoolTimeout:  time.Duration(cfg.PoolTimeout) * time.Second,
			Password:     cfg.Password,
			DB:           cfg.DB,
		}
	}

	client := redis.NewClient(opts)
	result := client.Ping(context.Background())
	if result.Err() != nil {
		return nil, result.Err()
	}

	return client, nil
}
