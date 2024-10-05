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
	benchtime   = flag.Duration("benchtime", 300*time.Second, "Bench time")
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

	var startTime = time.Now()
	for created < limit {
		var eta time.Time
		if created > 0 {
			remaininigMs := (time.Since(startTime)).Milliseconds() * int64(limit-created) / int64(created)
			eta = time.Now().Add(time.Duration(remaininigMs) * time.Millisecond)
		}
		slog.Info("filling table", "table", table, "limit", limit, "created", created, "eta", eta)
		eg := errgroup.Group{}
		eg.SetLimit(64)
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

	var err error

	var accountBalance int64
	err = qb.Select(accountTable).Columns("abalance").Where(qb.EqNamed("aid", "aid")).Query(session).
		BindMap(map[string]interface{}{"aid": aid}).
		Scan(&accountBalance)
	if err != nil {
		panic(err)
	}

	var tellerBalance int64
	err = qb.Select(tellerTable).Columns("tbalance").Where(qb.EqNamed("tid", "tid")).Query(session).
		BindMap(map[string]interface{}{"tid": tid}).
		Scan(&tellerBalance)
	if err != nil {
		panic(err)
	}

	var branchBalance int64
	err = qb.Select(branchTable).Columns("bbalance").Where(qb.EqNamed("bid", "bid")).Query(session).
		BindMap(map[string]interface{}{"bid": bid}).
		Scan(&branchBalance)
	if err != nil {
		panic(err)
	}

	//UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;
	adelta := rand.Int64N(10000) - 5000
	batch := session.NewBatch(gocql.LoggedBatch)
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
}

func read(session gocqlx.Session) {
	aid := rand.IntN(*scale * 100_000)
	var queriedBalance int
	err := qb.Select(accountTable).Columns("abalance").Where(qb.EqNamed("aid", "aid")).Query(session).Consistency(gocql.One).
		//RoutingKey([]byte(strconv.Itoa(aid))).
		BindMap(map[string]interface{}{"aid": aid}).
		Scan(&queriedBalance)
	if err != nil {
		panic(err)
	}
}

//one node rwmode
//progress:    591752 iterations     54 sec,    12038.555 tps
//progress:    604598 iterations     55 sec,    12843.228 tps
//progress:    616497 iterations     56 sec,    11900.213 tps
//3 nodes no batch
//progress:   2102675 iterations     92 sec,    24597.420 tps 5.199 lat
//progress:   2128653 iterations     93 sec,    25975.540 tps 4.928 lat
//progress:   2151865 iterations     94 sec,    23207.516 tps 5.515 lat

func main() {
	flag.Parse()

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	cluster := gocql.NewCluster("10.201.0.2")
	cluster.Timeout = 0
	cluster.Consistency = gocql.Quorum
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	cluster.WriteCoalesceWaitTime = 0
	cluster.WriteTimeout = 0

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
			fmt.Printf("progress: %9d iterations %6d sec, %12.3f tps %0.3f lat \n", finishIterations, int64(elapsed), tps, float64(iterLength.Milliseconds())/tps*float64(*concurrency))
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
