.PHONY: setup up down logs run test build clean clean-local

setup:
	@test -f config.yaml || cp examples/config.example.yaml config.yaml
	@test -f users.yaml || cp examples/users.example.yaml users.yaml

up: setup
	@docker compose up --build -d

down:
	@docker compose down

logs:
	@docker compose logs -f loommq

run: setup
	@./gradlew run --args="--config config.yaml"

test:
	@./gradlew test

build:
	@./gradlew build

clean:
	@docker compose down -v

clean-local:
	@./gradlew clean
	@rm -rf data
