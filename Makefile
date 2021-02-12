all: venv install done

venv:
	python3.8 -m venv venv
	venv/bin/pip3.8 install --upgrade setuptools

.PHONY: install
install: venv
	./venv/bin/pip install -U pip wheel
	./venv/bin/pip install -e .[dev]

.PHONY: done
done:
	@ echo "Installation finished succesfully."

README.html: README.adoc
	asciidoctor -n README.adoc

.PHONY: test
test: install
	./venv/bin/pytest

.PHONY: clean
clean:
	rm -rf .pytest_cache .eggs *.egg-info
	find . -path ./venv -prune -o -name "*.pyc" -o -name "*.pyo" -o -name "__pycache__" -print0 | xargs -r -0 rm -rv
	@echo "You may not want to remove ./venv, please do it by hand." >&2
