import sys
import os
import pandas as pd

from pyspark import SparkContext, SparkConf

print ">>>>>>> python version: " + str(sys.version)
print ">>>>>>> python version info: " + str(sys.version_info)

def nltk_parse(line):
	import nltk
	tokens = nltk.word_tokenize(line)
	pos = nltk.pos_tag(tokens)
	return pos

# detect simple alphanumeric words
def nltk_is_valid_word(pos):
	import re

	ret = False
	pos_word = pos[0]
	pos_type = pos[1]

	# check if pos represents a simple alphanumeric word
	if (re.search("^[0-9a-zA-Z]+$", pos_word) is not None):
		# found a valid word
		ret = True

	return ret

# param pos: Tuple(word, part-of-speech)
def nltk_isNoun(pos):
	# NN means Noun
	return pos[1] == "NN"

# param pos: Tuple(word, part-of-speech)
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
	#  - result is RDD of Tuples2 (word, part-of-speech)
	words_pos = sc.textFile(filename).flatMap(lambda line: nltk_parse(line))

	# subtract out Stop Words from NLTK
	import nltk
	sw_list = nltk.corpus.stopwords.words('english')
	sw = sc.parallelize(zip(sw_list, [1] * len(sw_list)))
	words_pos = words_pos.subtractByKey(sw)

	# do word count, and reduce dataset size by keeping only words above threshold
	#   - get count via map-reduce with key=(word,pos), value=count
	#   - only take counts above threshold 
	#	  (note: depends heavily on pos tagging accuracy, since same word tagged as different pos would fall off here)
	#   - sort by Count in Descending order by doing swap, sortByKey, then another swap
	#	- take only top 200 words, since only print out a few later
	#   - tuple (Key=(word, pos), Value=count)

	top_words = words_pos.map(lambda pos: ( (pos[0],pos[1]), 1)).reduceByKey( lambda v1,v2: v1+v2 ) \
						 .filter(lambda t:t[1] >= int(threshold)) \
						 .map(lambda t:( t[1],t[0]) ).sortByKey(False, 1).map( lambda t:( t[1],t[0]) ) \
						 .take(500)

	# words_count.cache()

	# get count of only Nouns
	#	- remember each item is ((word,pos), count)
	#	- remove non-Nouns, do this first since it's fast
	#   - remove meaningless and malformed words, do this near last step since it's slow
	#	- take only top 500 items, since next 2 steps are slow. this is enough since we only print out top 25 later.

	noun_counts = [ word for word in top_words if ( nltk_isNoun(word[0]) and nltk_is_valid_word(word[0]) ) ]

							 
	
	df_nouns = pd.DataFrame(dict(noun_counts).items(), columns=['Word','Count']) \
				 .sort_values('Count', ascending=False)

	print "\n\n"
	print "<<<<<<<<<< Top nouns that appear at least " + threshold + " times: "
	print df_nouns.head(25)

	# get count of only Verbs
	#	- remove non-Nouns, do this first since it's fast
	#   - remove meaningless and malformed words, do this near last step since it's slow
	verb_counts = [word for word in top_words if (nltk_isVerb(word[0]) and nltk_is_valid_word(word[0]))]
	
	df_verbs = pd.DataFrame(dict(verb_counts).items(), columns=['Word','Count']) \
				 .sort_values('Count', ascending=False)

	print "\n\n"
	print "<<<<<<<<<< Top verbs that appear at least " + threshold + " times: "
	print df_verbs.head(25)

if __name__ == "__main__":
	main(sys.argv[1:])
