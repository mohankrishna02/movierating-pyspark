from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *

sc = SparkContext(master="local",appName="movie rating")
sc.setLogLevel("ERROR")

ss = SparkSession.builder.getOrCreate()

#reading a movie csv file
movie = ss.read.format("csv").option("header","true").load("D:/BigData/Sparkfolder/PySpark/Projects/pythonProject/Datasets/movies.csv")
movie.show(truncate=False)
#reading a rating csv file
rating = ss.read.format("csv").option("header","true").load("D:/BigData/Sparkfolder/PySpark/Projects/pythonProject/Datasets/ratings.csv")
rating.show()

# joining the two dataframes movie & rating using inner join and movie_id column
joindata = movie.join(rating, on="movie_id",how="inner")
joindata.show(truncate=False)

#filter the movies whose ratings are greater than 8
topmovies = joindata.filter(col("ratings") >= 8.0)
topmovies.show(truncate=False)

#sorting the movies based on ratings
sorttopmovies = topmovies.orderBy(desc("ratings"))
sorttopmovies.show(truncate=False)

#writing the data as a parquet file
sorttopmovies.coalesce(1).write.format("parquet").save("D:/BigData/Sparkfolder/PySpark/Projects/pythonProject/movie-rating-parquet-pyspark")