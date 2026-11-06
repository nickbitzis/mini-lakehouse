.PHONY: install test lint fmt generate run demo gate-demo lineage clean

HOME_DIR ?= lakehouse
LH = uv run minilakehouse --home $(HOME_DIR)

install:
	uv sync

test:
	uv run pytest -q

lint:
	uv run ruff check .

generate:
	$(LH) generate --days 7

run:
	$(LH) run

lineage:
	$(LH) lineage --out docs

# The incremental story end to end: 6 days, rerun (no change), day 7 only, backfill, lineage.
demo: clean generate
	$(LH) run --through 2026-11-01
	$(LH) run --through 2026-11-01
	$(LH) run
	$(LH) backfill --from 2026-10-30 --to 2026-10-31
	$(LH) lineage --out docs

# A bad upstream day: the silver gate fails and gold keeps its previous version.
gate-demo: clean
	$(LH) generate --days 7 --corrupt-last-day
	$(LH) run --through 2026-11-01
	-$(LH) run

clean:
	rm -rf $(HOME_DIR)
