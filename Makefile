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
	./node_modules/.bin/mocha -u bdd --exit -t $(MOCHA_TIMEOUT) $(TEST_SUITE) ${MOCHA_ARGS}

test-all: jshint test

coverage:
	./node_modules/nyc/bin/nyc.js npm test

.PHONY: check test coverage
