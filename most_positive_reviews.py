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

for i in tokens.take(10):
    print(i)
