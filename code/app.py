from flask import Blueprint
main = Blueprint('main', __name__)
 
import json
from engine import RecommendationEngine
 
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
from flask import Flask, request

@main.route("/<int:userId>/ratings/<int:movieId>", methods=["GET"])
def movie_ratings(userId, movieId):
    logger.debug("User %s rating requested for anime %s", userId, movieId)
    ratings = recommendation_engine.get_ratings_for_movie_ids(userId, movieId)
    clear = json.dumps(ratings)
    # clear=str(clear)
    # clear2 = clear.replace('\"','"').replace('{"0":'," ").replace("},",",").replace("}}","}")
    return clear2
    # userId = str(userId)
    # movieId = str(movieId)
    # return userId+" "+movieId

@main.route("/<int:userId>/top/ratings/<int:movieCount>", methods=["GET"])
def movie_top_ratings(userId,movieCount):
    ratings = recommendation_engine.get_top_ratings(userId,movieCount)

    return json.dumps(ratings)

 
 

 
 
def create_app(spark_context, dataset_path):
    global recommendation_engine 

    recommendation_engine = RecommendationEngine(spark_context, dataset_path)    
    
    app = Flask(__name__)
    app.register_blueprint(main)
    return app