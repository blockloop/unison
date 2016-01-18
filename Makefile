SRC=$(shell find src/github.com/blockloop/unison -type f)

.PHONY: all
all: vendor build

.PHONY: vendor
vendor:
ifeq ("$(wildcard vendor/src/*)", "")
	gb vendor restore
endif

.PHONY: all
build: bin/unison

bin/unison: $(SRC)
	gb build github.com/blockloop/unison

