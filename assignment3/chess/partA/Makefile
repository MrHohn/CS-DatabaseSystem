PROJECT_DIR    = /user/$(USER)/chess/
INPUT_DIR      = $(PROJECT_DIR)/input
OUTPUT_DIR     = $(PROJECT_DIR)/output
CLASSPATH      = $(shell hadoop classpath)

default: clean
	@echo "--- now compile the programs and package to jar file ---"
	mkdir -p classes
	javac -classpath $(CLASSPATH) -Xlint:deprecation \
 	      -d classes *.java
	jar -cvf chess.jar -C classes .
	@echo "--- done ---"

run: chess.jar input
	hadoop jar chess.jar WordCount \
	       $(INPUT_DIR) $(OUTPUT_DIR)

cat: 
	hdfs dfs -cat $(OUTPUT_DIR)/*

dir:
	hdfs dfs -test -e $(PROJECT_DIR) || hdfs dfs -mkdir $(PROJECT_DIR)
	hdfs dfs -test -e $(INPUT_DIR) || hdfs dfs -mkdir $(INPUT_DIR)
	hdfs dfs -rm -f -r $(OUTPUT_DIR)

input: dir
	hdfs dfs -test -e $(INPUT_DIR)/file01 \
	  || hdfs dfs -put ../input/chess/file01 $(INPUT_DIR)/file01
	hdfs dfs -test -e $(INPUT_DIR)/file02 \
	  || hdfs dfs -put ../input/chess/file02 $(INPUT_DIR)/file02

clean-dir:
	hdfs dfs -rm -f -r $(INPUT_DIR)
	hdfs dfs -rm -f -r $(OUTPUT_DIR)
	hdfs dfs -rm -r -f $(PROJECT_DIR)

clean:
	@echo "--- now clean compiled files --- "
	rm -rf classes *.jar

.PHONY: clean clean-dir cat dir input clean run default
