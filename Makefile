.PHONY: test clean

test: sqlite
	python3 db_test.py

sqlite: main.go $(wildcard db/*)
	go build -o sqlite main.go

clean:
	rm sqlite
	rm users.db