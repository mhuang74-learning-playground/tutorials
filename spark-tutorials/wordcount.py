import sys
import os
import pandas as pd

from pyspark import SparkContext, SparkConf

print ">>>>>>> python version: " + str(sys.version)
print ">>>>>>> python version info: " + str(sys.version_info)

FILTERED_WORDS = ['the', 'and', 'of', 'to', 'and', 'that', 'in', 'a', 'for', 'is', 'be', 'it', 'was', 'are', 'were', 'this']

def main(argv):

	filename = argv[0]
	threshold = argv[1]

	print "Counting Words: threshold=" + threshold + ", file=" + filename

	conf = SparkConf().setAppName("Spark Count")
	sc = SparkContext(conf=conf)

	tokenized = sc.textFile(filename).flatMap(lambda line: line.split(" "))

	wordCounts = tokenized.map(lambda word: (word,1)).reduceByKey(lambda v1,v2: v1+v2)

	filtered = wordCounts.filter(lambda t:t[1] >= int(threshold) and t[0].lower() not in FILTERED_WORDS)
	filtered.cache()

	wc_filtered = filtered.collect()

	sorted = filtered.map(lambda t:(t[1],t[0])).sortByKey(False, 2).map(lambda t:(t[1],t[0]))
	sorted.cache()

	wc = sorted.collect()
	
	for t in wc[0:50]: print t

	df = pd.DataFrame(dict(wc_filtered).items(), columns=['Word','Count'])
	df = df.sort_values('Count', ascending=False)
	df_top_20 = df.head(20)
	print df_top_20

if __name__ == "__main__":
	main(sys.argv[1:])
