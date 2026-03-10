PYTHON ?= python3

.PHONY: install build test

install:
	$(PYTHON) -m pip install -e .

build:
	PYTHONPATH=src $(PYTHON) -m eurostat_pipeline build-all

test:
	PYTHONPATH=src $(PYTHON) -m unittest discover -s tests
