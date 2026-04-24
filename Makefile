PYTHON ?= python3
PIP ?= pip

.PHONY: install test clean

install:
	$(PIP) install -e .

test:
	$(PYTHON) -m unittest discover -s tests

clean:
	rm -rf build dist .pytest_cache *.egg-info
