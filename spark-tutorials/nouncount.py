import sys
import os
import pandas as pd

from pyspark import SparkContext, SparkConf

print ">>>>>>> python version: " + str(sys.version)
print ">>>>>>> python version info: " + str(sys.version_info)

FILTERED_WORDS = ['the', 'and', 'of', 'to', 'and', 'that', 'in', 'a', 'for', 'is', 'be', 'it', 'was', 'are', 'were', 'this']

def nltk_parse(line):
	import nltk
	tokens = nltk.word_tokenize(line)
	pos = nltk.pos_tag(tokens)
	return pos

def nltk_isNoun(pos):
	import re
	from nltk.corpus import stopwords

	ret = False
	pos_word = pos[0]
	pos_type = pos[1]

	# check if pos represents a Noun & a valid Word
	if pos_type == "NN" \
	and pos_word not in stopwords.words('english') \
	and re.search("^[0-9a-zA-Z]+$", pos_word) is not None:
		# only include those  that look like regular alphanumeric words
		ret = True

	return ret

def main(argv):

	filename = argv[0]
	threshold = argv[1]

	print "Counting Nouns: threshold=" + threshold + ", file=" + filename

	conf = SparkConf().setAppName("Spark Count - Nouns")
	sc = SparkContext(conf=conf)

	# RDD of Tuples2 (word, pos)
	part_of_speech = sc.textFile(filename).flatMap(lambda line: nltk_parse(line))

	nouns_pos = part_of_speech.filter(lambda pos: nltk_isNoun(pos))

	noun_counts = nouns_pos.map(lambda pos: (pos[0],1)).reduceByKey(lambda v1,v2: v1+v2)
	noun_counts.cache()

	filtered_noun_counts = noun_counts.filter(lambda t:t[1] >= int(threshold) and t[0].lower() not in FILTERED_WORDS)
	filtered_noun_counts.cache()

	wc_filtered = filtered_noun_counts.collect()

	# sort by Count in Descending order
	sorted_filtered_noun_counts = filtered_noun_counts.map(lambda t:(t[1],t[0])).sortByKey(False, 2).map(lambda t:(t[1],t[0]))
	sorted_filtered_noun_counts.cache()

	wc = sorted_filtered_noun_counts.collect()
	
	df = pd.DataFrame(dict(sorted_filtered_noun_counts.take(500)).items(), columns=['Word','Count'])
	df = df.sort_values('Count', ascending=False)
	df_top = df.head(25)
	print "<<<<<<<<<< Top nouns that appear at least " + threshold + " times: "
	print df_top

if __name__ == "__main__":
	main(sys.argv[1:])
