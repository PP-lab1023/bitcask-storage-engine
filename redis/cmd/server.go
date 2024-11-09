package main

import (
	kvproject "bitcask-go"
	kvproject_redis "bitcask-go/redis"
	"log"
	"sync"

	"github.com/tidwall/redcon"
)

const addr = "127.0.0.1:6380"

type BitcaskServer struct{	
	dbs map[int]*kvproject_redis.RedisDataStructure   // Redis can be connected to many databases
	server *redcon.Server
	mu sync.RWMutex
}

func main() {
	// Open Redis service
	redisDataStructure, err := kvproject_redis.NewRedisDataStructure(kvproject.DefaultOptions)
	if err != nil {
		panic(err)
	}

	// Initialize BitcaskServer
	bitcaskServer := &BitcaskServer{
		dbs: make(map[int]*kvproject_redis.RedisDataStructure),
	}
	bitcaskServer.dbs[0] = redisDataStructure // Default

	// Initialize Redis server
	bitcaskServer.server = redcon.NewServer(addr, execClientCommand, bitcaskServer.accept, bitcaskServer.close)
	bitcaskServer.listen()
}

func (svr *BitcaskServer) listen() {
	log.Panicln("bitcask server running, ready to accept connections")
	_ = svr.server.ListenAndServe()
}

func (svr *BitcaskServer) accept(conn redcon.Conn) bool {
	cli := new(BitcaskClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()

	cli.server = svr
	cli.db = svr.dbs[0]

	conn.SetContext(cli)
	return true
}

func (svr *BitcaskServer) close(conn redcon.Conn, err error) {
	for _, db := range svr.dbs {
		_ = db.Close()
	}
	_ = svr.server.Close()
}