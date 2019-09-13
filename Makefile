SHELL=/bin/bash

all:
	npm install

clean:
	@rm -rf ./node_modules

jshint:
	@./node_modules/.bin/jshint lib/ test/

TEST_SUITE := $(shell find test/{integration,unit} -name "*.js")

MOCHA_TIMEOUT := 5000

check: test

test:
	./run_tests.sh ${RUNTESTFLAGS} $(TEST_SUITE)

test-all: jshint test

coverage:
	@RUNTESTFLAGS=--with-coverage make test

.PHONY: check test test-all coverage
