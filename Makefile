# project specific definitions
SRCDIR = cmd
PACKAGES = $(shell cd "$(SRCDIR)" && echo *)
BINDIR = bin

# go commands
GOCMD = go
GOBUILD = $(GOCMD) build
GOCLEAN = $(GOCMD) clean
GOTEST = $(GOCMD) test
GOVET = $(GOCMD) vet
GOFMT = $(GOCMD) fmt
GOGET = $(GOCMD) get
GOMOD = $(GOCMD) mod

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
	@for pkg in $(PACKAGES) ; do \
		printf "compile: %-10s  env: [%s]  flags: [%s]\n" "$${pkg}" "$(GOENV)" "$(GOFLAGS)" ; \
		$(GOENV) $(GOBUILD) $(GOFLAGS) -o "$(BINDIR)/$${pkg}.$(TARGET)" "$(SRCDIR)/$${pkg}"/*.go ; \
	done

symlink:
	@for pkg in $(PACKAGES) ; do \
		echo "symlink: $${pkg}" ; \
		ln -sf "$${pkg}.$(TARGET)" "$(BINDIR)/$${pkg}" ; \
	done

darwin: target-darwin build

target-darwin:
	$(eval TARGET = darwin)

linux: target-linux build

target-linux:
	$(eval TARGET = linux)

rebuild: flag build

flag:
	$(eval GOFLAGS_EXTRA += -a)

rebuild-darwin: target-darwin rebuild

rebuild-linux: target-linux rebuild

fmt:
	@for pkg in $(PACKAGES) ; do \
		echo "fmt: $${pkg}" ; \
		(cd "$(SRCDIR)/$${pkg}" && $(GOFMT)) ; \
	done

vet:
	@for pkg in $(PACKAGES) ; do \
		echo "vet: $${pkg}" ; \
		(cd "$(SRCDIR)/$${pkg}" && $(GOVET)) ; \
	done

clean:
	rm -rf $(BINDIR)
	@for pkg in $(PACKAGES) ; do \
		echo "clean: $${pkg}" ; \
		(cd "$(SRCDIR)/$${pkg}" && $(GOCLEAN)) ; \
	done

dep:
	$(GOGET) -u
	$(GOMOD) tidy
