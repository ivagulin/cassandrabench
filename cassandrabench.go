package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/olekukonko/tablewriter"
	"github.com/samber/lo"
	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/qb"
	"golang.org/x/sync/errgroup"
	"log"
	"log/slog"
	"math/rand/v2"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var (
	concurrency = flag.Int("concurrency", 128, "Number of concurrent goroutines")
	benchtime   = flag.Duration("benchtime", 3600*time.Second, "Bench time")
	scale       = flag.Int("scale", 1000, "Scaling factor")
	RWMode      = flag.Bool("rwmode", false, "Read write mode")
	initMode    = flag.Bool("init", false, "init")
)

var (
	accountTable = "bench.pgbench_accounts"
	tellerTable  = "bench.pgbench_tellers"
	branchTable  = "bench.pgbench_branches"
	historyTable = "bench.pgbench_history"
)

type Account struct {
	AID      int   `db:"aid"`
	BID      int64 `db:"bid"`
	Abalance int64 `db:"abalance"`
}

type Teller struct {
	TID      int   `db:"tid"`
	BID      int64 `db:"bid"`
	Tbalance int64 `db:"tbalance"`
}

type Branche struct {
	BID      int   `db:"bid"`
	Bbalance int64 `db:"bbalance"`
}

type History struct {
	TID   int64     `db:"tid"`
	BID   int64     `db:"bid"`
	AID   int64     `db:"aid"`
	Delta int64     `db:"delta"`
	Mtime time.Time `db:"mtime"`
}

func fillTable(session gocqlx.Session, table string, limit int, keycolumn string, valcolumn string) {
	var err error
	created := 0

	//it := qb.Select(table).Columns(keycolumn).Query(session).Iter()
	//var id int
	//for it.Scan(&id) {
	//	if id > created {
	//		created = id
	//	}
	//}
	//err := it.Close()
	//if err != nil {
	//	panic(err)
	//}

	for created < limit {
		slog.Info("filling table", "table", table, "limit", limit, "created", created)
		eg := errgroup.Group{}
		eg.SetLimit(*concurrency)
		for _ = range 100 {
			batch := session.NewBatch(gocql.UnloggedBatch)
			for it := created; it < limit && it-created < 100; it++ {
				q := qb.Insert(table).Columns(keycolumn, valcolumn).Query(session).Consistency(gocql.Any)
				err = batch.BindMap(q, map[string]interface{}{keycolumn: it, valcolumn: 0})
				if err != nil {
					panic(err)
				}
			}
			eg.Go(func() error {
				return session.ExecuteBatch(batch)
			})
			created += 100
		}
		err = eg.Wait()
		if err != nil {
			panic(err)
		}
	}
}

func fill(session gocqlx.Session) {
	accountsToCreate := *scale * 100_000
	tellersToCreate := *scale * 10
	branchesToCreate := *scale * 1

	fillTable(session, accountTable, accountsToCreate, "aid", "abalance")

	fillTable(session, tellerTable, tellersToCreate, "tid", "tbalance")

	fillTable(session, branchTable, branchesToCreate, "bid", "bbalance")
}

func readWrite(session gocqlx.Session) {
	aid := rand.IntN(*scale * 100_000)
	tid := rand.IntN(*scale * 10)
	bid := rand.IntN(*scale * 1)
	adelta := rand.Int64N(10000) - 5000

	var accountBalance, tellerBalance, branchBalance int64
	var err error

	err = qb.Select(accountTable).Columns("abalance").Where(qb.EqNamed("aid", "aid")).Query(session).
		BindMap(map[string]interface{}{"aid": aid}).
		Scan(&accountBalance)
	if err != nil {
		panic(err)
	}

	err = qb.Select(tellerTable).Columns("tbalance").Where(qb.EqNamed("tid", "tid")).Query(session).
		BindMap(map[string]interface{}{"tid": tid}).
		Scan(&tellerBalance)
	if err != nil {
		panic(err)
	}

	err = qb.Select(branchTable).Columns("bbalance").Where(qb.EqNamed("bid", "bid")).Query(session).
		BindMap(map[string]interface{}{"bid": bid}).
		Scan(&branchBalance)
	if err != nil {
		panic(err)
	}

	batch := session.NewBatch(gocql.LoggedBatch)
	//UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
	aq := qb.Update(accountTable).SetNamed("abalance", "abalance").Where(qb.EqNamed("aid", "aid")).Query(session)
	err = batch.BindMap(aq, map[string]interface{}{"aid": aid, "abalance": accountBalance + adelta})
	if err != nil {
		panic(err)
	}

	//UPDATE pgbench_tellers SET tbalance = tbalance + :delta WHERE tid = :tid;
	tq := qb.Update(tellerTable).SetNamed("tbalance", "tbalance").Where(qb.EqNamed("tid", "tid")).Query(session)
	err = batch.BindMap(tq, map[string]interface{}{"tid": tid, "tbalance": tellerBalance + adelta})
	if err != nil {
		panic(err)
	}

	//UPDATE pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid;
	bq := qb.Update(branchTable).SetNamed("bbalance", "bbalance").Where(qb.EqNamed("bid", "bid")).Query(session)
	err = batch.BindMap(bq, map[string]interface{}{"bid": bid, "bbalance": branchBalance + adelta})
	if err != nil {
		panic(err)
	}

	//INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);
	hq := qb.Insert(historyTable).Columns("tid", "bid", "aid", "delta", "mtime").Query(session)
	err = batch.BindStruct(hq, History{
		AID:   int64(aid),
		TID:   int64(tid),
		BID:   int64(bid),
		Delta: int64(adelta),
		Mtime: time.Now(),
	})
	if err != nil {
		panic(err)
	}

	err = session.ExecuteBatch(batch)
	if err != nil {
		panic(err)
	}

	//SELECT abalance FROM pgbench_accounts WHERE aid = :aid;
	var queriedBalance int
	err = qb.Select(accountTable).Columns("abalance").Where(qb.EqNamed("aid", "aid")).Query(session).
		BindMap(map[string]interface{}{"aid": aid}).
		Scan(&queriedBalance)
	if err != nil {
		panic(err)
	}
}

func read(session gocqlx.Session) {
	aid := rand.IntN(*scale * 100_000)
	var queriedBalance int
	err := qb.Select(accountTable).Columns("abalance").Where(qb.EqNamed("aid", "aid")).Query(session).Consistency(gocql.One).
		BindMap(map[string]interface{}{"aid": aid}).
		Scan(&queriedBalance)
	if err != nil {
		panic(err)
	}
}

func main() {
	flag.Parse()

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	cluster := gocql.NewCluster("localhost:9042", "localhost:9043", "localhost:9044")
	cluster.Timeout = 60 * time.Second
	cluster.Consistency = gocql.One
	session, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	t0 := `CREATE KEYSPACE IF NOT EXISTS bench WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}`

	t1 := `
	CREATE TABLE IF NOT EXISTS bench.pgbench_accounts (
		aid int PRIMARY KEY,
		bid int,
		abalance int,
		fillter text
	)`

	t2 := `
	CREATE TABLE IF NOT EXISTS bench.pgbench_tellers (
		tid int PRIMARY KEY,
		bid int,
		tbalance int,
		filler text
	)`

	t3 := `
	CREATE TABLE IF NOT EXISTS bench.pgbench_branches (
		bid int PRIMARY KEY,
		bbalance int,
		filler text
	)`

	t4 := `
	CREATE TABLE IF NOT EXISTS bench.pgbench_history (
		tid int,
		bid int,
		aid int,
		delta int,
		mtime timestamp,
		filler text,
		PRIMARY KEY (tid, bid, aid, mtime)
	)`

	for _, t := range []string{t0, t1, t2, t3, t4} {
		err = session.ExecStmt(t)
		if err != nil {
			panic(err)
		}
	}

	slog.Info("filling...", "scale", *scale)
	if *initMode {
		fill(session)
	}

	slog.Info("testing...")
	var iterations uint64
	var conflicts uint64
	finishTimer, cancelFunc := context.WithTimeout(context.Background(), *benchtime)
	defer cancelFunc()
	var wg sync.WaitGroup
	wg.Add(*concurrency)
	startTime := time.Now()
	for _ = range *concurrency {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-finishTimer.Done():
					return
				default:
					atomic.AddUint64(&iterations, 1)
					if *RWMode {
						readWrite(session)
					} else {
						read(session)
					}
				}
			}
		}()
	}

	ticker := time.Tick(time.Second)
	for time.Since(startTime) < *benchtime {
		startIterTime := time.Now()
		startIterations := atomic.LoadUint64(&iterations)
		select {
		case <-ticker:
			iterLength := time.Since(startIterTime)
			finishIterations := atomic.LoadUint64(&iterations)
			tps := float64(finishIterations-startIterations) / (iterLength.Seconds())
			elapsed := time.Since(startTime).Truncate(time.Second).Seconds()
			//slog.Info("results", "iterations", iterations, "elapsed", .Seconds(), "tps", tpsString)
			fmt.Printf("intermediate results: %9d iterations %6d sec, %12.3f tps\n", finishIterations, int64(elapsed), tps)
		}
	}
	wg.Wait()

	slog.Info("throughtput results", "concurrency", *concurrency, "iterations", iterations, "conflicts", conflicts)
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Name", "Latency(us)", "Throughput(rps)"})
	table.SetBorder(false)
	table.SetHeaderLine(false)
	table.SetRowLine(false)
	testName := lo.Ternary(*RWMode, "tpcb-like", "tpcb-readonly")
	latency := fmt.Sprintf("%0.3f", float64(benchtime.Microseconds())/float64(iterations)*float64(*concurrency))
	throughput := fmt.Sprintf("%0.3f", float64(iterations)/benchtime.Seconds())
	table.Append([]string{testName, latency, throughput})
	table.Render()
}
