.PHONY: humbill create-env-vanilla poetry cleanup lint new test again

SHELL := /usr/bin/zsh

AIRFLOW_VERSION=2.3.4
PYTHON_VERSION="$$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

cleanup:
	@rm -rf .venv poetry.lock pyproject.toml

create-env-vanilla:
	@echo "Creating virtual environment..."
	@python3 -m venv .venv
	@source .venv/bin/activate
	@pip install apache-airflow==${AIRFLOW_VERSION} --constraint ${CONSTRAINT_URL}
	@pip install -r requirements.txt

humbill:
	@echo "Refreshing Humbill files..."
	@rm -rf ./plugins/humbill
	@git clone https://github.com/grupo-sbf/data-eng-libs.git --branch dev ./tmp/
	@cp -R ./tmp/humbill/ ./plugins/
	@rm -rf ./tmp

poetry:
	@poetry config virtualenvs.in-project true
	@poetry init -n
	@poetry add apache-airflow==${AIRFLOW_VERSION} --extras=${CONSTRAINT_URL}
	@cat requirements.txt | xargs poetry add
	@poetry shell

lint:
	@echo "Check lintting at './dags'"
	@find dags -maxdepth 7 -type f | grep .py | pyupgrade --py38-plus
	@autoflake --in-place --remove-unused-variables --remove-all-unused-imports --recursive ./dags
	@isort ./dags --profile black
	@black ./dags
	@echo "Check lintting at './plugins'"
	@find plugins -maxdepth 7 -type f | grep .py | pyupgrade --py38-plus
	@autoflake --in-place --remove-unused-variables --remove-all-unused-imports --recursive ./plugins
	@isort ./plugins --profile black
	@black ./plugins


new:
	cp -r ../base .
	rm -rf .git
	git init
	git remote add origin git@github.com:rodrigomcoelho/pipeline.git
	git fetch --all
	git add .
	git commit -m "make environment ready for composer"
	git push origin main

again:
	git checkout main
	git branch -D dev
	git push origin -d dev
	git checkout -b dev

test:
	cp -r ../data-eng-fis-cliente/dags/refined ./dags
	git add .
	git commit -m "chore: validate cicd pipeline"
	git push origin dev

