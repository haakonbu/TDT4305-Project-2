from pyspark import SparkContext, SparkConf
import base64
import re

sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf=sparkConf)

folder_name = "./data/"
input_reviewers = "yelp_top_reviewers_with_reviews.csv.gz"
output_file = "result2.tsv"

# Create RDD object from .gz-file
reviewersRDD = sc.textFile(folder_name + input_reviewers)
rdd = reviewersRDD.map(lambda line: line.split('\t'))

# Filter out header
header = rdd.first()
rdd = rdd.filter(lambda line: line != header)


def lower_clean_string(string):
    """
    Function used to make string lowercase filter out punctuation

    :param string: Text string to strip clean and make lower case.
    :return:
    """

    punctuation = '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~[\n]'
    lowercase_string = string.lower()
    for character in punctuation:
        lowercase_string = lowercase_string.replace(character, '')
    return lowercase_string


rdd = rdd.map(lambda fields: [fields[2], lower_clean_string(base64.b64decode(fields[3]))])
tokens = rdd.map(lambda line: [line[0], line[1].split(" ")])    # Tokenize the reviews
tokens = tokens.map(lambda x: [x[0], filter(None, x[1])])       # Filter out empty strings

# Filter out stopwords:
file = open('stopwords.txt', 'r')
stop_words = file.read().split('\n')

tokens = tokens.map(lambda x: [x[0], (sc.parallelize(x[1])).filter(lambda y: y not in stop_words)])
# Need to fix line above. Tried to make an rdd of the second column with sc.parallelize

for i in tokens.take(2):
    print(i)


#load AFINN-111 into dictionary as described in README
afinn = dict(map(lambda (k,v): (k,int(v)),
                     [ line.split('\t') for line in open("AFINN-111.txt") ]))


# Example on how to use: sum(map(lambda word: afinn.get(word, 0), "Rainy day but still in a good mood".lower().split()))

scores = tokens.map(lambda rev: [rev[0], sum(map(lambda word: afinn.get(word, 0), rev[1]))])



# ALT 2. comment out from line 35 to use this
# use flatmap for removing stop words
"""
rdd = rdd.map(lambda fields: [fields[2], lower_clean_string(base64.b64decode(fields[3])), 'linesplitter'])
rdd = rdd.flatMap(lambda line: line.split(" "))
filtered = rdd.filter(lambda y: y not in stop_words)
mapped = filtered.map(lambda line: line.split('linesplitter'))

scores = tokens.map(lambda rev: [rev[0], sum(map(lambda word: afinn.get(word, 0), rev[1:]))])

"""