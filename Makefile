IMAGE ?= lead-enrichment
PORT ?= 8000

.PHONY: install run test docker-build docker-run docker-stop

install:
	uv sync

run:
	uv run uvicorn app.main:app --host 0.0.0.0 --port $(PORT) --reload

test:
	uv run pytest

docker-build:
	docker build -t $(IMAGE) .

docker-run:
	docker run --rm -p $(PORT):8000 --name $(IMAGE) $(IMAGE)

docker-stop:
	- docker stop $(IMAGE)

