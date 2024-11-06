package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

type OperationResult struct {
	operationNumber int
	value           string
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	store map[string]string
	cache map[int]*OperationResult
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, exists := kv.store[args.Key]
	if !exists {
		value = ""
	}

	reply.Value = value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if operationResult, exists := kv.cache[args.ClientId]; exists && args.OperationNumber == operationResult.operationNumber {
		reply.Value = operationResult.value
		return
	}

	kv.store[args.Key] = args.Value
	reply.Value = kv.store[args.Key]

	if operationResult, exists := kv.cache[args.ClientId]; exists {
		operationResult.operationNumber = args.OperationNumber
		operationResult.value = args.Value
	} else {
		kv.cache[args.ClientId] = &OperationResult{operationNumber: args.OperationNumber, value: args.Value}
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if operationResult, exists := kv.cache[args.ClientId]; exists && args.OperationNumber == operationResult.operationNumber {
		reply.Value = operationResult.value
		return
	}

	oldValue, ok := kv.store[args.Key]
	if !ok {
		oldValue = ""
	}

	kv.store[args.Key] = oldValue + args.Value
	reply.Value = oldValue

	if operationResult, exists := kv.cache[args.ClientId]; exists {
		operationResult.operationNumber = args.OperationNumber
		operationResult.value = oldValue
	} else {
		kv.cache[args.ClientId] = &OperationResult{operationNumber: args.OperationNumber, value: oldValue}
	}
}

func (kv *KVServer) Ack(args *AckArgs, reply *AckReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.cache, args.ClientId)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.store = make(map[string]string)
	kv.cache = make(map[int]*OperationResult)
	return kv
}
