import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark import SparkContext, SparkConf

# To avoid error with ascii-encoding
reload(sys)
sys.setdefaultencoding('utf-8')

# Configure Spark
sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf=sparkConf)
spark = SparkSession.builder.appName("Yelp").config("spark.some.config.option", "some-value").getOrCreate()

# Set data folder, inputs and output
folder_name = "./data/"
input_reviewers = "yelp_top_reviewers_with_reviews.csv.gz"
output_file = "result.tsv"


#Load into separate dataframe
def load_dataframe(input_file, delimiter):
    """
    Reads in a file with a given delimiter and returns a dataframe
    :param input_file: File to read
    :param delimiter: Delimiter separating values
    :return: Dataframe created from file
    """
    rdd = sc.textFile(folder_name + input_file).map(lambda l: l.split(delimiter))  # First load RDD
    header = rdd.first()                            # Get header for dataframe
    fields = [StructField(field_name, StringType(), True) for field_name in header]
    schema = StructType(fields)                     # Make schema
    rdd = rdd.filter(lambda row: row != header)     # Filter out header to avoid duplicates
    df = spark.createDataFrame(rdd, schema=schema)  # Create dataframe with schema
    return df


df_reviewers = load_dataframe(input_reviewers, "\t")    # Load reviewers into dataframe



