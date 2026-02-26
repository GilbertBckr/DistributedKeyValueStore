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
	if err != nil {
		log.Error("Error loading .env file")
	}

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

	servicediscoveryModule := servicediscovery.NewEnvServiceDiscovery()

	twoPhaseCommitCoordinator := &twophasecommitcoordinator.TwoPhaseCommit{
		PersistenceManager: sqliteModule,
		SDiscovery:         servicediscoveryModule, // we will set this later after we initialize the service discovery module
	}

	twoPhaseCommitParticipant := &twophasecommitparticipant.TwoPhaseCommitParticipant{
		PersistenceManager: sqliteModule,
	}

	// Setup background runners
	ctxPhase1, cancelPhase1 := context.WithCancel(context.Background())
	phase1Runner := twophasecommitcoordinator.GetNewPhase1Runner(sqliteModule, servicediscoveryModule)
	go phase1Runner(ctxPhase1)
	defer cancelPhase1()

	ctxPhase2, cancelPhase2 := context.WithCancel(context.Background())
	phase2Runner := twophasecommitcoordinator.GetNewPhase2Runner(sqliteModule, servicediscoveryModule)
	go phase2Runner(ctxPhase2)
	defer cancelPhase2()

	server.StartServer(twoPhaseCommitCoordinator, twoPhaseCommitParticipant)

}
