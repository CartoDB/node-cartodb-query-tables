SHELL=/bin/bash

all:
	npm install

clean:
	@rm -rf ./node_modules

lint:
	@./node_modules/.bin/eslint lib/**/*.js test/**/*.js

TEST_SUITE := $(shell find test/{integration,unit} -name "*.js")

MOCHA_TIMEOUT := 5000

check: test

test:
	./run_tests.sh ${RUNTESTFLAGS} $(TEST_SUITE)

test-all: lint test

coverage:
	@RUNTESTFLAGS=--with-coverage make test

.PHONY: check lint test test-all coverage
