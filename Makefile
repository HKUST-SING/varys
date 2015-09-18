all:
	./sbt/sbt package

clean:
	./bin/stop-all.sh

run: all clean
	./bin/start-all.sh

.PHONY: all clean run
