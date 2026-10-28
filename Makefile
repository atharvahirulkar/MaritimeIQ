PYTHON ?= $(CURDIR)/.venv/bin/python
ifeq ($(wildcard $(PYTHON)),)
  PYTHON := $(shell command -v python3.11 || command -v python3.10 || command -v python3)
endif
# Spark needs Java 17/21: keep an existing JAVA_HOME, else find Homebrew / system JDKs
ifeq ($(strip $(JAVA_HOME)),)
  JAVA_HOME := $(firstword $(wildcard /opt/homebrew/opt/openjdk@17 /opt/homebrew/opt/openjdk@21 /usr/local/opt/openjdk@17 /usr/local/opt/openjdk@21 /opt/homebrew/opt/openjdk) $(shell /usr/libexec/java_home 2>/dev/null))
endif
export JAVA_HOME
export PATH := $(JAVA_HOME)/bin:$(PATH)
export MARITIMEIQ_PYTHON := $(PYTHON)
export PYSPARK_PYTHON := $(PYTHON)
export PYSPARK_DRIVER_PYTHON := $(PYTHON)

.PHONY: install demo eval test ui clean sync-gcs

install:
	@if [ ! -x "$(CURDIR)/.venv/bin/python" ]; then $(PYTHON) -m venv $(CURDIR)/.venv; fi
	$(CURDIR)/.venv/bin/python -m pip install -U pip setuptools wheel
	$(CURDIR)/.venv/bin/python -m pip install -r requirements.txt
	$(CURDIR)/.venv/bin/python -m pip install -e .
	@echo "Use: make demo  (Python via .venv)"

demo:
	$(PYTHON) scripts/run_demo.py

eval:
	PYTHONPATH=src $(PYTHON) -m maritimeiq.eval.harness

test:
	PYTHONPATH=src $(PYTHON) -m pytest tests -q

ui:
	PYTHONPATH=src $(PYTHON) -m streamlit run src/maritimeiq/ui/app.py

clean:
	rm -rf lakehouse/bronze/* lakehouse/silver/* lakehouse/gold/* lakehouse/context/* lakehouse/checkpoints/* .eval_results

sync-gcs:
	./scripts/sync_gcs.sh
