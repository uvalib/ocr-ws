# project specific definitions
PROJECT = ocr-ws
SRCDIR = cmd/$(PROJECT)
BINDIR = bin

# go commands
GOCMD = go
GOBUILD = $(GOCMD) build
GOCLEAN = $(GOCMD) clean
GOVET = $(GOCMD) vet
GOFMT = $(GOCMD) fmt

# default build target is host machine architecture
MACHINE = $(shell uname -s | tr '[A-Z]' '[a-z]')
TARGET = $(MACHINE)

# darwin-specific definitions
GOENV_darwin = 
GOFLAGS_darwin = 

# linux-specific definitions
GOENV_linux = CGO_ENABLED=0
GOFLAGS_linux = -installsuffix cgo

# extra flags
GOENV_EXTRA = GOARCH=amd64
GOFLAGS_EXTRA = 

# default target:

build: target compile symlink

target:
	$(eval GOENV = GOOS=$(TARGET) $(GOENV_$(TARGET)) $(GOENV_EXTRA))
	$(eval GOFLAGS = $(GOFLAGS_$(TARGET)) $(GOFLAGS_EXTRA))

compile:
	$(GOENV) $(GOBUILD) $(GOFLAGS) -o $(BINDIR)/$(PROJECT).$(TARGET) $(SRCDIR)/*.go

symlink:
	ln -sf $(PROJECT).$(TARGET) $(BINDIR)/$(PROJECT)

build-darwin: target-darwin build

target-darwin:
	$(eval TARGET = darwin)

build-linux: target-linux build

target-linux:
	$(eval TARGET = linux)

rebuild: flag build

flag:
	$(eval GOFLAGS_EXTRA += -a)

rebuild-darwin: target-darwin rebuild

rebuild-linux: target-linux rebuild

fmt:
	(cd $(SRCDIR) && $(GOFMT))

vet:
	(cd $(SRCDIR) && $(GOVET))

clean:
	$(GOCLEAN)
	rm -rf $(BINDIR)

dep:
	dep ensure
	dep status
