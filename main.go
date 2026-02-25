package main

import (
	"context"
	"distributedKeyValue/persistence"
	"distributedKeyValue/server"
	servicediscovery "distributedKeyValue/service_discovery"
	twophasecommitcoordinator "distributedKeyValue/two_phase_commit_coordinator"
	twophasecommitparticipant "distributedKeyValue/two_phase_commit_participant"
	"log/slog"
	"os"
	"time"

	"github.com/charmbracelet/log"

	"github.com/joho/godotenv"
)

const transactionStatesPath = "transactionsStates.gob"

func main() {

	err := godotenv.Load()

	ownName := os.Getenv("NODE_NAME")

	clog := log.NewWithOptions(os.Stderr, log.Options{
		ReportTimestamp: true,         // Ensure this is true
		TimeFormat:      time.Kitchen, // "3:04PM" or use time.RFC3339
		Level:           log.DebugLevel,
	})

	// 2. Wrap it in slog
	logger := slog.New(clog)
	slog.SetDefault(logger)

	sqliteModule := persistence.MustNewSqliteTransactionManagerPersistence(ownName + "data.sqlite")

	if err != nil {
		panic(err)
	}
	servicediscoveryModule := servicediscovery.NewEnvServiceDiscovery()

	twoPhaseCommitCoordinator := &twophasecommitcoordinator.TwoPhaseCommit{
		PersistenceManager: sqliteModule,
		SDiscovery:         servicediscoveryModule, // we will set this later after we initialize the service discovery module
	}

	twoPhaseCommitParticipant := &twophasecommitparticipant.TwoPhaseCommitParticipant{
		PersistenceManager: sqliteModule,
	}

	ctx, cancel := context.WithCancel(context.Background())

	phase1Runner := twophasecommitcoordinator.GetNewPhase1Runner(sqliteModule, servicediscoveryModule)

	go phase1Runner(ctx)

	defer cancel()

	server.StartServer(twoPhaseCommitCoordinator, twoPhaseCommitParticipant)

}
