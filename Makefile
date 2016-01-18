SRC=$(shell find src/github.com/blockloop/unison -type f)

.PHONY: all
all: vendor build

.PHONY: vendor
vendor:
	if [[ -z "$(ls vendor/src/*)" ]] ; \
	then \
		gb vendor restore
	fi;

.PHONY: all
build: bin/unison

bin/unison: $(SRC)
	gb build github.com/blockloop/unison

