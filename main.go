package main

import (
	"distributedKeyValue/persistence"
	"distributedKeyValue/server"
	"os"

	"github.com/joho/godotenv"
)

const transactionStatesPath = "transactionsStates.gob"

func main() {

	err := godotenv.Load()

	ownName := os.Getenv("NODE_NAME")

	sqliteModule, err := persistence.NewSqlitePersistence(ownName + "data.sqlite")

	if err != nil {
		panic(err)
	}

	server.StartServer(sqliteModule)

}
