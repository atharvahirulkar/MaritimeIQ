PYTHON ?= $(CURDIR)/.venv/bin/python
ifeq ($(wildcard $(PYTHON)),)
  PYTHON := /opt/homebrew/bin/python3.10
endif
export MARITIMEIQ_PYTHON := $(PYTHON)
export PYSPARK_PYTHON := $(PYTHON)
export PYSPARK_DRIVER_PYTHON := $(PYTHON)

.PHONY: install demo eval test api web ui reset

install:
	@if [ ! -x "$(CURDIR)/.venv/bin/python" ]; then /opt/homebrew/bin/python3.10 -m venv $(CURDIR)/.venv; fi
	$(CURDIR)/.venv/bin/python -m pip install -U pip setuptools wheel
	$(CURDIR)/.venv/bin/python -m pip install -r requirements.txt
	$(CURDIR)/.venv/bin/python -m pip install -e .
	cd web && npm install

# Unattended run: agent suggestions stand in for HITL decisions.
demo:
	$(PYTHON) scripts/run_demo.py

eval:
	PYTHONPATH=src $(PYTHON) -m maritimeiq.eval.harness

test:
	PYTHONPATH=src $(PYTHON) -m pytest tests -q

# API only (pair with `make web` for hot-reload development).
api:
	PYTHONPATH=src $(PYTHON) -m uvicorn maritimeiq.api.app:app --port 8000

# Vite dev server on :5173, proxying /api to :8000.
web:
	cd web && npm run dev

# Build the SPA and serve UI + API together on http://127.0.0.1:8000
ui:
	cd web && npm run build
	PYTHONPATH=src $(PYTHON) -m uvicorn maritimeiq.api.app:app --port 8000

# Keeps data/ and Bronze; clears everything activated after Bronze.
reset:
	rm -rf lakehouse/silver/* lakehouse/gold/* lakehouse/context/* .eval_results
