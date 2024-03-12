ifeq ($(OS),Windows_NT)
    path := $(shell echo %cd%)
else
    path := $(shell pwd)
endif

mockup:
	docker run --rm -v "$(path)":/src -w /src vektra/mockery --all