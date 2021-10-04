# project specific definitions
SRCDIR = cmd
BINDIR = bin
PACKAGE = ocr-ws

# go commands
GOCMD = go
GOBLD = $(GOCMD) build
GOCLN = $(GOCMD) clean
GOTST = $(GOCMD) test
GOVET = $(GOCMD) vet
GOFMT = $(GOCMD) fmt
GOGET = $(GOCMD) get
GOMOD = $(GOCMD) mod
GOVER = $(GOCMD) version
GOLNT = golint
GOBIN = $(HOME)/go/bin

# default build target is host machine architecture
MACHINE = $(shell uname -s | tr '[A-Z]' '[a-z]')
TARGET = $(MACHINE)

# git commit used for this build, either passed to make via Dockerfile or determined from local directory
ifeq ($(GIT_COMMIT),)
	GIT_COMMIT = $(shell \
		commit="$$(git rev-list -1 HEAD 2>/dev/null)" ; \
		if [ "$${commit}" != "" ] ; then \
			postfix="" ; \
			git diff --quiet || postfix="-modified" ; \
			echo "$${commit}$${postfix}" ; \
		fi \
	)
endif

# darwin-specific definitions
GOENV_darwin =
GOFLAGS_darwin =
GOLINK_darwin =

# linux-specific definitions
GOENV_linux =
GOFLAGS_linux =
GOLINK_linux =

# extra flags
GOENV_EXTRA = GOARCH=amd64
GOFLAGS_EXTRA =
GOLINK_EXTRA = -X main.gitCommit=$(GIT_COMMIT)

# default target:

build: go-vars compile symlink

go-vars:
	$(eval GOENV = GOOS=$(TARGET) $(GOENV_$(TARGET)) $(GOENV_EXTRA))
	$(eval GOFLAGS = $(GOFLAGS_$(TARGET)) $(GOFLAGS_EXTRA))
	$(eval GOLINK = -ldflags '$(GOLINK_$(TARGET)) $(GOLINK_EXTRA)')

compile:
	@ \
	echo "building [$(PACKAGE)] for target: [$(TARGET)]" ; \
	echo ; \
	$(GOVER) ; \
	echo ; \
	printf "compile: %-6s  env: [%s]  flags: [%s]  link: [%s]\n" "$(PACKAGE)" "$(GOENV)" "$(GOFLAGS)" "$(GOLINK)" ; \
	$(GOENV) $(GOBLD) $(GOFLAGS) $(GOLINK) -o "$(BINDIR)/$(PACKAGE).$(TARGET)" "$(SRCDIR)"/*.go || exit 1

symlink:
	@ \
	echo ; \
	echo "symlink: $(BINDIR)/$(PACKAGE) -> $(PACKAGE).$(TARGET)" ; \
	ln -sf "$(PACKAGE).$(TARGET)" "$(BINDIR)/$(PACKAGE)" || exit 1

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
#	$(eval GOENV_EXTRA += CGO_ENABLED=0)
	$(eval GOLINK_EXTRA += -extldflags "-static")

docker: docker-vars linux

rebuild-docker: docker-vars rebuild-linux

# maintenance rules
fmt:
	@ \
	echo "[FMT] $(PACKAGE)" ; \
	(cd "$(SRCDIR)" && $(GOFMT))

vet:
	@ \
	echo "[VET] $(PACKAGE)" ; \
	(cd "$(SRCDIR)" && $(GOVET))

lint:
	@ \
	echo "[LINT] $(PACKAGE)" ; \
	(cd "$(SRCDIR)" && $(GOLNT))

clean:
	@ \
	echo "[PURGE] $(BINDIR)/" ; \
	rm -rf $(BINDIR) ; \
	echo "[CLEAN] $(PACKAGE)" ; \
	(cd "$(SRCDIR)" && $(GOCLN))

tidy:
	@ \
	echo "[MOD] $(GOMOD) tidy" ; \
	$(GOMOD) tidy

verify:
	@ \
	echo "[MOD] $(GOMOD) verify" ; \
	$(GOMOD) verify

dep:
	@ \
	echo "[MOD] GOPROXY=$(GOPROXY) $(GOGET)" ; \
	GOPROXY=$(GOPROXY) $(GOGET) -u ./$(SRCDIR)/...

DEP: goproxy-direct dep

goproxy-direct:
	$(eval GOPROXY = direct)

check-static:
	@ \
	echo "[CHECK] static checks" ; \
	go install honnef.co/go/tools/cmd/staticcheck ; \
	$(GOBIN)/staticcheck -checks all,-S1002,-ST1003 -fail all,-U1000 ./$(SRCDIR)/...

check-shadow:
	@ \
	echo "[CHECK] shadowed variables" ; \
	go install golang.org/x/tools/go/analysis/passes/shadow/cmd/shadow ; \
	go vet -vettool=$(GOBIN)/shadow ./$(SRCDIR)/...

check: check-shadow check-static

sure: check tidy verify fmt vet lint
