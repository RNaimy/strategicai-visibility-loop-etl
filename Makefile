# Makefile for StrategicAI Visibility Loop ETL (Public Demo)
# Usage:
#   make setup        → Create .venv and install dependencies
#   make run          → Run the ETL merge script inside .venv
#   make clean        → Remove generated files
#   make freeze       → Export current dependencies to requirements.txt

.PHONY: setup run clean freeze

PYTHON = .venv/bin/python
PIP = .venv/bin/pip

setup:
	@echo "Creating virtual environment and installing dependencies..."
	python3 -m venv .venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@echo "Setup complete. Virtual environment ready."

run:
	@echo "Running ETL merge to generate merged/merged_visibility.csv..."
	$(PYTHON) etl_merge.py

clean:
	@echo "Cleaning output files..."
	rm -rf __pycache__ merged/*.csv

freeze:
	@echo "Exporting dependencies to requirements.txt..."
	$(PIP) freeze > requirements.txt