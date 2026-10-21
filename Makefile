.PHONY: help install data test test-spark glue-local lint tf-fmt tf-validate deploy upload destroy

TF_DIR  := infra/terraform
SPARK   := apache/spark:3.5.3-python3

help:  ## Show available targets
	@grep -E "^[a-z-]+:.*##" $(MAKEFILE_LIST) | awk -F ":.*## " '{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install:  ## Install dev dependencies
	pip install -e ".[dev]"

data:  ## Regenerate synthetic carrier files in data/landing
	python scripts/generate_sample_data.py

test:  ## Lambda tests (moto) - no AWS account needed
	pytest

test-spark:  ## Glue PySpark tests inside a Spark container (no local Java needed)
	docker run --rm -u root -v "$(CURDIR):/work" -w /work $(SPARK) bash -c \
	  "pip install -q pytest && PYTHONPATH=/opt/spark/python:$$(ls /opt/spark/python/lib/py4j-*.zip) python3 -m pytest -p no:cacheprovider tests/test_glue_transforms.py"

glue-local:  ## Run the Glue job locally on the sample data -> ./output
	docker run --rm -u root -v "$(CURDIR):/work" -w /work $(SPARK) bash -c \
	  "PYTHONPATH=/opt/spark/python:$$(ls /opt/spark/python/lib/py4j-*.zip) python3 src/glue_jobs/shipment_events_etl.py --local --input_path 'data/landing/*/*.csv' --output_root output"

lint:  ## Ruff lint
	ruff check .

tf-fmt:  ## Terraform format check
	terraform -chdir=$(TF_DIR) fmt -check -recursive

tf-validate:  ## Terraform validate
	terraform -chdir=$(TF_DIR) init -backend=false && terraform -chdir=$(TF_DIR) validate

deploy:  ## Provision / update all AWS resources
	terraform -chdir=$(TF_DIR) init && terraform -chdir=$(TF_DIR) apply

upload:  ## Upload sample files to the landing zone (triggers the pipeline)
	aws s3 sync data/landing "s3://$$(terraform -chdir=$(TF_DIR) output -raw datalake_bucket)/landing/"

destroy:  ## Tear everything down
	terraform -chdir=$(TF_DIR) destroy
