package main

import (
	"distributedKeyValue/persistence"
	"distributedKeyValue/server"
	servicediscovery "distributedKeyValue/service_discovery"
	twophasecommit "distributedKeyValue/two_phase_commit"
	"os"

	"github.com/joho/godotenv"
)

const transactionStatesPath = "transactionsStates.gob"

func main() {

	err := godotenv.Load()

	ownName := os.Getenv("NODE_NAME")

	sqliteModule := persistence.MustNewSqliteTransactionManagerPersistence(ownName + "data.sqlite")

	if err != nil {
		panic(err)
	}

	twoPhaseCommit := &twophasecommit.TwoPhaseCommit{
		PersistenceManager: sqliteModule,
		SDiscovery:         servicediscovery.NewEnvServiceDiscovery(), // we will set this later after we initialize the service discovery module
	}

	server.StartServer(twoPhaseCommit)

}
