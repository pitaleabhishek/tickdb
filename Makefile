PYTHON ?= python3
PIP ?= pip

.PHONY: install test demo benchmark-baseline benchmark-block-index benchmark-native-scan clean

install:
	$(PIP) install -e .

test:
	$(PYTHON) -m unittest discover -s tests

demo:
	$(PYTHON) examples/demo.py

benchmark-baseline:
	$(PYTHON) benchmarks/run_layout_baselines.py

benchmark-block-index:
	$(PYTHON) benchmarks/run_block_index_comparison.py

benchmark-native-scan:
	$(PYTHON) benchmarks/run_native_scan_comparison.py

clean:
	rm -rf build dist .pytest_cache *.egg-info
