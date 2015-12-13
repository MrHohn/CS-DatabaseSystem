PROJECT_DIR    = /user/$(USER)/chessA
INPUT_DIR      = /user/$(USER)/input/chess
TEMP_DIR       = $(PROJECT_DIR)/temp
OUTPUT_DIR     = $(PROJECT_DIR)/output
CLASSPATH      = $(shell hadoop classpath)

default: clean
	@echo "--- now compile the programs and package to jar file ---"
	mkdir -p classes
	hadoop com.sun.tools.javac.Main -d classes *.java
	jar -cvf chessA.jar -C classes .
	@echo "--- done ---"

run: chessA.jar input
	hadoop jar chessA.jar ChessA $(INPUT_DIR) $(TEMP_DIR) $(OUTPUT_DIR)

cat: 
	hdfs dfs -cat $(OUTPUT_DIR)/*

cat-temp: 
	hdfs dfs -cat $(TEMP_DIR)/*

dir:
	hdfs dfs -test -e $(PROJECT_DIR) || hdfs dfs -mkdir $(PROJECT_DIR)
	hdfs dfs -test -e $(INPUT_DIR) || hdfs dfs -mkdir -p $(INPUT_DIR)
	hdfs dfs -rm -f -r $(TEMP_DIR)
	hdfs dfs -rm -f -r $(OUTPUT_DIR)

input: dir
	hdfs dfs -test -e $(INPUT_DIR)/ficsgamesdb_small_201501.pgn \
	  || hdfs dfs -put ../../input/chess/ficsgamesdb_small_201501.pgn $(INPUT_DIR)/ficsgamesdb_small_201501.pgn
	hdfs dfs -test -e $(INPUT_DIR)/ficsgamesdb_small_201502.pgn \
	  || hdfs dfs -put ../../input/chess/ficsgamesdb_small_201502.pgn $(INPUT_DIR)/ficsgamesdb_small_201502.pgn
	hdfs dfs -test -e $(INPUT_DIR)/ficsgamesdb_small_201503.pgn \
	  || hdfs dfs -put ../../input/chess/ficsgamesdb_small_201503.pgn $(INPUT_DIR)/ficsgamesdb_small_201503.pgn

clean-dir:
	hdfs dfs -rm -f -r $(INPUT_DIR)
	hdfs dfs -rm -f -r $(TEMP_DIR)
	hdfs dfs -rm -f -r $(OUTPUT_DIR)
	hdfs dfs -rm -r -f $(PROJECT_DIR)

clean:
	@echo "--- now clean compiled files --- "
	rm -rf classes *.jar

.PHONY: clean clean-dir cat dir input clean run default
