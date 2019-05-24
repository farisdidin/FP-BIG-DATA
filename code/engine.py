import os
import findspark
findspark.init("/usr/local/spark")
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import pandas

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RecommendationEngine:
    """A anime recommendation engine
    """

    def __train_model(self,ratings):
        """Train the ALS model with the current dataset
        """

        logger.info("Training the ALS model...")
        self.als = ALS(rank=12, maxIter=21, regParam=0.16, userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop", nonnegative = True)
        self.model = self.als.fit(self.ratings_list[0])
        
        logger.info("ALS model built!")
        model = self.model
        return model


    def get_ratings_for_movie_ids(self, userId, movieId, model):
        """Given a user_id and a list of anime_ids, predict ratings for them 
        """

        dataframe = self.spark.createDataFrame([(userId, movieId)], ["userId", "movieId"])
        predictions = self.model_list[model].transform(dataframe)
        ratings = predictions.toPandas()
        ratings = ratings.to_json()

        return ratings
    
    def get_top_ratings(self, userId, movies_count, model):
        """Recommends up to movies_count top unrated movies to user_id
        """
        users = self.ratings_list[model].select(self.als.getUserCol()).distinct()
        users = users.filter(users.userId == userId)
        top_ratings = self.model.recommendForUserSubset(users,movies_count)

        self.json_top = top_ratings.toPandas()
        self.json_top = self.json_top.to_json()
        return self.json_top



    def __init__(self, spark, dataset_path):
        """Init the recommendation engine given a Spark context and a dataset path
        """
        self.ratings_list = []
        self.model_list = []
        

        logger.info("Starting up the Recommendation Engine: ")

        self.spark = spark

        # Load ratings data for later use
        logger.info("Loading Ratings data...")
        
        listCsv = os.listdir(dataset_path)
        listCsv = list(listCsv)
        listCsv.sort()
        for i in listCsv:
            print(i)
            ratings_file_path = os.path.join(dataset_path, i)
            ratings = spark.read.csv(ratings_file_path, header=True, inferSchema=True).limit(1000000)
            self.ratings_list.append(ratings)
            model = self.__train_model(ratings)
            self.model_list.append(model)

        
