all: pylint mypy test
	true

pylint:
	pylint --exit-zero *py tests/*py --disable invalid-name,missing-function-docstring,missing-module-docstring

mypy:
	mypy *py tests/*py 

test:
	python -m pytest tests

autopep8:
	for p in *py; do autopep8 -i $$p; done
