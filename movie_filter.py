from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import desc
import pyspark.sql.functions as F

S3_DATA_SOURCE_PATH_1 = 's3://movie-data-bucket-1234/data-source/basics-data.tsv'
S3_DATA_SOURCE_PATH_2 = 's3://movie-data-bucket-1234/data-source/rating-data.tsv'
S3_DATA_SOURCE_PATH_3 = 's3://movie-data-bucket-1234/data-source/akas-data.tsv'
S3_DATA_OUTPUT_PATH = 's3://movie-data-bucket-1234/data-source/movie-data-filtered'


def main():
    spark = SparkSession.builder.appName('SparkApp').getOrCreate()
    basicsData = spark.read.csv(S3_DATA_SOURCE_PATH_1, sep=r'\t', header=True)
    ratingData = spark.read.csv(S3_DATA_SOURCE_PATH_2, sep=r'\t', header=True)
    akasData = spark.read.csv(S3_DATA_SOURCE_PATH_3, sep=r'\t', header=True)
    sB = basicsData.where((col('startYear') == 2021) & (col('titleType') == 'movie'))
    sR = ratingData.where((col('numVotes') >= 1000))
    sA = akasData.where((col('region')) == "US")

    
    joinedS1 = sB.join(sR, ["tconst"], 'inner')
    joinedS2 = joinedS1.withColumn("tconst", col("tconst")).join(sA.withColumn("tconst", col("titleId")), on="tconst")


    selectedS = joinedS2.select("primaryTitle", "runtimeMinutes", "genres", "averageRating", "numVotes")
    selectedS.write.mode('overwrite').csv(S3_DATA_OUTPUT_PATH, sep=r'\t')
    print('Selected data was successfully saved to s3: %s' % S3_DATA_OUTPUT_PATH)

if __name__ == '__main__':
    main()