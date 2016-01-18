
all: vendor build

vendor:
	gb vendor restore

build:
	gb build github.com/blockloop/unison

