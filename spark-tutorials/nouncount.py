import sys
import os
import pandas as pd

from pyspark import SparkContext, SparkConf

print ">>>>>>> python version: " + str(sys.version)
print ">>>>>>> python version info: " + str(sys.version_info)

def nltk_parse_meaningful_words(line):
	import nltk

	# tokenize line
	tokens = nltk.word_tokenize(line)
	# get part-of-speech metadata
	all_pos = nltk.pos_tag(tokens)

	import re
	from nltk.corpus import stopwords

	ret = []

	# filter out stopwords and non-words
	for pos in all_pos:
		pos_word = pos[0]
		pos_type = pos[1]

		# check if pos represents a valid alphanumeric Word that is not a Stop Word
		if (pos_word not in stopwords.words('english') and \
			re.search("^[0-9a-zA-Z]+$", pos_word) is not None):
			# found a meaninful word
			ret.append(pos)

	return ret

def nltk_isNoun(pos):
	# NN means Noun
	return pos[1] == "NN"

def nltk_isVerb(pos):
	# VERB means Verb
	return pos[1] == "VERB" or pos[1].startswith("VB")


def main(argv):

	filename = argv[0]
	threshold = argv[1]

	print "Counting Nouns: threshold=" + threshold + ", file=" + filename

	conf = SparkConf().setAppName("Spark Count - Nouns")
	sc = SparkContext(conf=conf)


	# use NLTK to tokenize lines, then parse out only meaningful words
	#  - each line is broken up by nltk_parse to collection of tokens. hence use flatMap
	#  - than remove meaningless and malformed words
	#  - result is RDD of Tuples2 (word, part-of-speech)
	meaningful_words_pos = sc.textFile(filename).flatMap(lambda line: nltk_parse_meaningful_words(line))
	meaningful_words_pos.cache()

	# get count of only Nouns
	#   - get count via map-reduce with key=word, value=count
	#   - only take counts above threshold 
	#   - sort by Count in Descending order by doing swap, sortByKey, then another swap
	noun_counts = meaningful_words_pos.filter(lambda pos: nltk_isNoun(pos)) \
									  .map(lambda pos: (pos[0],1)).reduceByKey(lambda v1,v2: v1+v2) \
									  .filter(lambda t:t[1] >= int(threshold)) \
									  .map(lambda t:(t[1],t[0])).sortByKey(False, 2).map(lambda t:(t[1],t[0]))
	
	
	df_nouns = pd.DataFrame(dict(noun_counts.take(500)).items(), columns=['Word','Count']) \
				 .sort_values('Count', ascending=False)

	print "\n\n"
	print "<<<<<<<<<< Top nouns that appear at least " + threshold + " times: "
	print df_nouns.head(25)

	# get count of only Verbs
	#   - get count via map-reduce with key=word, value=count
	#   - only take counts above threshold 
	#   - sort by Count in Descending order by doing swap, sortByKey, then another swap
	verb_counts = meaningful_words_pos.filter(lambda pos: nltk_isVerb(pos)) \
									  .map(lambda pos: (pos[0],1)).reduceByKey(lambda v1,v2: v1+v2) \
									  .filter(lambda t:t[1] >= int(threshold)) \
									  .map(lambda t:(t[1],t[0])).sortByKey(False, 2).map(lambda t:(t[1],t[0]))
	
	
	df_verbs = pd.DataFrame(dict(verb_counts.take(500)).items(), columns=['Word','Count']) \
				 .sort_values('Count', ascending=False)

	print "\n\n"
	print "<<<<<<<<<< Top verbs that appear at least " + threshold + " times: "
	print df_verbs.head(25)

if __name__ == "__main__":
	main(sys.argv[1:])
