PYTHON ?= python3
PIP ?= pip

.PHONY: install test demo benchmark-layout benchmark-block-pruning benchmark-native-scan benchmark-baseline benchmark-block-index clean

install:
	$(PIP) install -e .

test:
	$(PYTHON) -m unittest discover -s tests

demo:
	$(PYTHON) examples/demo.py

benchmark-layout:
	$(PYTHON) benchmarks/run_layout_benchmarks.py

benchmark-block-pruning:
	$(PYTHON) benchmarks/run_block_pruning_benchmarks.py

benchmark-native-scan:
	$(PYTHON) benchmarks/run_native_scan_benchmarks.py

benchmark-baseline: benchmark-layout

benchmark-block-index: benchmark-block-pruning

clean:
	rm -rf build dist .pytest_cache *.egg-info
