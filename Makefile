PYTHON ?= $(CURDIR)/.venv/bin/python
ifeq ($(wildcard $(PYTHON)),)
  PYTHON := /opt/homebrew/bin/python3.10
endif
export MARITIMEIQ_PYTHON := $(PYTHON)
export PYSPARK_PYTHON := $(PYTHON)
export PYSPARK_DRIVER_PYTHON := $(PYTHON)

.PHONY: install demo eval test ui clean sync-gcs

install:
	@if [ ! -x "$(CURDIR)/.venv/bin/python" ]; then /opt/homebrew/bin/python3.10 -m venv $(CURDIR)/.venv; fi
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
