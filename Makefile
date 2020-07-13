STOREDIR = $(CURDIR)/storage
FLAGS    = -ldflags "-X github.com/vonaka/shreplic/server/smr.Storage=$(STOREDIR)"

all: system

install: | $(STOREDIR)
install:
	go build -o $(GOPATH)/bin/shr-client $(FLAGS) client/client.go
	go build -o $(GOPATH)/bin/shr-master $(FLAGS) master/master.go
	go build -o $(GOPATH)/bin/shr-server $(FLAGS) server/server.go

system: | $(STOREDIR)
system:
	go build -o bin/shr-client $(FLAGS) client/client.go
	go build -o bin/shr-master $(FLAGS) master/master.go
	go build -o bin/shr-server $(FLAGS) server/server.go

race: FLAGS += -race
race: system

$(STOREDIR):
	mkdir -p $(STOREDIR)
