all:
	./sbt/sbt package

clean:
	./bin/stop-all.sh

run: clean
	./bin/start-all.sh

.PHONY: all clean run
