# project specific definitions
SRCDIR = cmd
BINDIR = bin
PKGDOCKER = ocr-ws
PKGLAMBDA = lambda
PACKAGES = $(PKGDOCKER) $(PKGLAMBDA)

# go commands
GOCMD = go
GOBLD = $(GOCMD) build
GOCLN = $(GOCMD) clean
GOTST = $(GOCMD) test
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
GOENV_linux = 
GOFLAGS_linux = 

# extra flags
GOENV_EXTRA = GOARCH=amd64
GOFLAGS_EXTRA = 

# default target:

build: go-vars compile symlink

go-vars:
	$(eval GOENV = GOOS=$(TARGET) $(GOENV_$(TARGET)) $(GOENV_EXTRA))
	$(eval GOFLAGS = $(GOFLAGS_$(TARGET)) $(GOFLAGS_EXTRA))

compile:
	@ \
	echo "building packages: [$(PACKAGES)] for target: [$(TARGET)]" ; \
	echo ; \
	for pkg in $(PACKAGES) ; do \
		printf "compile: %-6s  env: [%s]  flags: [%s]\n" "$${pkg}" "$(GOENV)" "$(GOFLAGS)" ; \
		$(GOENV) $(GOBLD) $(GOFLAGS) -o "$(BINDIR)/$${pkg}.$(TARGET)" "$(SRCDIR)/$${pkg}"/*.go || exit 1 ; \
	done

symlink:
	@ \
	echo ; \
	for pkg in $(PACKAGES) ; do \
		echo "symlink: $(BINDIR)/$${pkg} -> $${pkg}.$(TARGET)" ; \
		ln -sf "$${pkg}.$(TARGET)" "$(BINDIR)/$${pkg}" || exit 1 ; \
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

# docker: make sure binary is linux and truly static
docker-vars:
	$(eval PACKAGES = $(PKGDOCKER))
	$(eval GOFLAGS_EXTRA += --ldflags '-extldflags "-static"')

docker: docker-vars linux

rebuild-docker: docker-vars rebuild-linux

# lambda: make sure binary is linux
lambda-vars:
	$(eval PACKAGES = $(PKGLAMBDA))

lambda: lambda-vars linux

rebuild-lambda: lambda-vars rebuild-linux

# maintenance rules
fmt:
	@ \
	for pkg in $(PACKAGES) ; do \
		echo "fmt: $${pkg}" ; \
		(cd "$(SRCDIR)/$${pkg}" && $(GOFMT)) ; \
	done

vet:
	@ \
	for pkg in $(PACKAGES) ; do \
		echo "vet: $${pkg}" ; \
		(cd "$(SRCDIR)/$${pkg}" && $(GOVET)) ; \
	done

clean:
	@ \
	echo "purge: $(BINDIR)/" ; \
	rm -rf $(BINDIR) ; \
	for pkg in $(PACKAGES) ; do \
		echo "clean: $${pkg}" ; \
		(cd "$(SRCDIR)/$${pkg}" && $(GOCLN)) ; \
	done

dep:
	$(GOGET) -u
	$(GOMOD) tidy
	$(GOMOD) verify
