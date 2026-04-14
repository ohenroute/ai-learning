PYTHON ?= python3
VENV := .venv
PIP := $(VENV)/bin/pip
RUN := $(VENV)/bin/python

.PHONY: install migrate migrate-db-only clean

install: $(VENV)/.installed

$(VENV)/.installed: requirements.txt
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@touch $@

migrate: $(VENV)/.installed
	$(RUN) migrate.py

migrate-db-only: $(VENV)/.installed
	$(RUN) migrate.py --db-only

clean:
	rm -rf $(VENV)
