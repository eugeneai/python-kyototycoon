clean:
	find . -name '*~' | xargs rm -f
	find . -name '*.pyc' | xargs rm -f
	rm -rf build/
	rm -rf tests/__pycache__
	rm -rf kyototycoon/__pycache__
