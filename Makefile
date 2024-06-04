# Development
format-all:
	isort . --skip setup.py && black --exclude setup.py .

install-all:
	poetry install

update-all:
	poetry update
	poetry export --without-hashes -f requirements.txt --output requirements.txt
