all: pylint mypy
	true

pylint:
	pylint *py --disable invalid-name,missing-function-docstring,missing-module-docstring

mypy:
	mypy *py

autopep8:
	for p in *py; do autopep8 -i $$p; done
