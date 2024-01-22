CLIENT_EXE=client
SERVER_EXE=kv-server
CLI_SRC_DIR=cmd/client
SVR_SRC_DIR=cmd/server
BIN_DIR=build/bin
REL_DIR=deployments
TEST_DIR=test

all: build_client build_server

build_client:
	mkdir -p ${BIN_DIR}
	go build -gcflags "-N -l" -o ${BIN_DIR}/${CLIENT_EXE} ${CLI_SRC_DIR}/main.go

build_server:
	mkdir -p ${BIN_DIR}
	go build -o ${BIN_DIR}/${SERVER_EXE} ${SVR_SRC_DIR}/main.go

release_client:
	docker build --tag client . -f ${CLI_SRC_DIR}/Dockerfile

release_server:
	docker build --tag kv-server . -f ${SVR_SRC_DIR}/Dockerfile

run_client: release_client
	docker container run --interactive client

run_server: release_server
	docker container run kv-server

test: build_client build_server
	${TEST_DIR}/localtests ${BIN_DIR}/client ${BIN_DIR}/kv-server ${BIN_DIR}/database
	rm -rf ${BIN_DIR}/database

clean:
	go clean
	rm -rf ${BIN_DIR}
	rm -rf ${TEST_DIR}/bin
	rm -f ${TEST_DIR}/logs.txt
	rm -f ${REL_DIR}/*
