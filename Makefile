.PHONY: data build bench dashboard test all
data:      ; ./scripts/download.sh
build:     ; python -m warehouse.run
bench:     ; python -m warehouse.bench
dashboard: ; python dashboard/build.py
test:      ; python -m pytest -q tests
all: build bench dashboard
