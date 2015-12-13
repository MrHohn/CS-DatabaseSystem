PROJECT_DIR    = /user/$(USER)/wordcount
INPUT_DIR      = /user/$(USER)/input/wordcount
OUTPUT_DIR     = $(PROJECT_DIR)/output
CLASSPATH      = $(shell hadoop classpath)

default: clean
	@echo "--- now compile the programs and package to jar file ---"
	mkdir -p classes
	hadoop com.sun.tools.javac.Main -d classes *.java
	jar -cvf wordcount.jar -C classes .
	@echo "--- done ---"

run: wordcount.jar input
	hadoop jar wordcount.jar WordCount \
	       $(INPUT_DIR) $(OUTPUT_DIR)

cat: 
	hdfs dfs -cat $(OUTPUT_DIR)/*

dir:
	hdfs dfs -test -e $(PROJECT_DIR) || hdfs dfs -mkdir $(PROJECT_DIR)
	hdfs dfs -test -e $(INPUT_DIR) || hdfs dfs -mkdir -p $(INPUT_DIR)
	hdfs dfs -rm -f -r $(OUTPUT_DIR)

input: dir
	hdfs dfs -test -e $(INPUT_DIR)/file01 \
	  || hdfs dfs -put ../input/wordcount/file01 $(INPUT_DIR)/file01
	hdfs dfs -test -e $(INPUT_DIR)/file02 \
	  || hdfs dfs -put ../input/wordcount/file02 $(INPUT_DIR)/file02

clean-dir:
	hdfs dfs -rm -f -r $(INPUT_DIR)
	hdfs dfs -rm -f -r $(OUTPUT_DIR)
	hdfs dfs -rm -r -f $(PROJECT_DIR)

clean:
	@echo "--- now clean compiled files --- "
	rm -rf classes *.jar

.PHONY: clean clean-dir cat dir input clean run default
