
# Calm down. We don't actually use make to build anything.  This is
# some complex commands I want to remember.

.PHONY: doc
doc:
	stack haddock --flag parallel-conduits:devel

.PHONY: test
test:
	stack test --coverage --flag parallel-conduits:devel
