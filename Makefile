.PHONY: data build bench dashboard dbt test all
data:      ; ./scripts/download.sh
build:     ; python -m warehouse.run
bench:     ; python -m warehouse.bench
dashboard: ; python dashboard/build.py
dbt:       ; cd dbt && DBT_PROFILES_DIR=. dbt build && DBT_PROFILES_DIR=. dbt docs generate --static && cp target/static_index.html ../docs/dbt/index.html
test:      ; python -m pytest -q tests
all: build bench dashboard
