from pyspark import SparkContext, SparkConf
import base64

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


rdd = rdd.map(lambda fields: [fields[2], base64.b64decode(fields[3]).lower()])
tokenize = rdd.flatMap(lambda line: [line[0], line[1].split(" ")])

