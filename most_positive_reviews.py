from pyspark import SparkContext, SparkConf
import base64
from operator import add

sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf=sparkConf)

file = open('stopwords.txt', 'r')
stop_words = file.read().split('\n')


def find_top_k_most_positive_reviews(folder_name, input_file, k):
    folder_name = folder_name
    input_file = input_file
    output_file = "results.tsv"

    # Create RDD object from .gz-file
    reviewersRDD = sc.textFile(folder_name + input_file)
    rdd = reviewersRDD.map(lambda line: line.split('\t'))

    # Filter out header
    header = rdd.first()
    rdd = rdd.filter(lambda line: line != header)

    rdd = rdd.map(lambda fields: [fields[2], lower_clean_string(base64.b64decode(fields[3]))])
    tokens = rdd.map(lambda line: [line[0], line[1].split(" ")])  # Tokenize the reviews
    tokens = tokens.map(lambda x: [x[0], filter(None, x[1])])  # Filter out empty strings

    tokens = tokens.map(lambda x: [x[0], filter(filter_out_stopwords, x[1])])  # Filter out stop words
    tokens = tokens.map(lambda x: [x[0], filter(filter_out_length_of_one, x[1])])  # Filter out short words

    # load AFINN-111 into dictionary as described in README
    afinn = dict(map(lambda (k, v): (k, int(v)),
                     [line.split('\t') for line in open("AFINN.txt")]))

    # Example on how to use: sum(map(lambda word: afinn.get(word, 0), "Rainy day but still in a good mood".lower().split()))
    scores = tokens.map(lambda rev: [rev[0], sum(map(lambda word: afinn.get(word, 0), rev[1]))])
    scores = scores.reduceByKey(add)    # Sum all the values

    scores.saveAsTextFile(folder_name + output_file)

    return scores.takeOrdered(k, key=lambda x: -x[1])


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


def filter_out_stopwords(word):
    if word not in stop_words:
        return True
    return False


def filter_out_length_of_one(word):
    if len(word) > 1:
        return True
    return False


if __name__ == "__main__":
    top_reviews = find_top_k_most_positive_reviews("./data/", "yelp_top_reviewers_with_reviews.csv.gz", 10)

    for i in top_reviews:
        print i
