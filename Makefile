.PHONY: format lint test

SRC := ./

format:
	poetry run autoflake --remove-all-unused-imports --remove-unused-variables --in-place --recursive $(SRC)
	poetry run isort $(SRC)
	poetry run ruff format $(SRC)
	
lint:
	poetry run ruff check $(SRC)
	
test:
	poetry run pytest -v --disable-warnings -p no:cacheprovider
