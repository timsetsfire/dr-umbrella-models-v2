import os
import yaml
import pandas as pd
from helper import *
from requests_futures.sessions import FuturesSession
from io import StringIO
from datarobot_drum import RuntimeParameters
import numpy as np
import logging 
import time
from pathlib import Path 
import pickle 
from concurrent.futures import ThreadPoolExecutor


logging.basicConfig(level=logging.INFO)
logging.basicConfig(
                format="{} - %(levelname)s - %(asctime)s - %(message)s".format("debug-loggers"),
        )
logger = logging.getLogger(__name__)
logger.setLevel("WARNING")


class RoutingModel(object):
    def __init__(self, code_dir: str):
        path = Path(code_dir) / "models"    
        with open(os.path.join(code_dir, "routing_config.yaml"), "r") as f:
            self.routing_config = yaml.load(f, Loader=yaml.FullLoader)
        self.models = {}
        for model_config in self.routing_config:
            quantile = model_config["tag"]
            model_path = path / model_config["tag"] / "model.pkl"
            with open( str(model_path), "rb") as f:
                model = pickle.load(f) 
                self.models[quantile] = model

    def predict(self, df):
        return [ (tag, model.predict(df).tolist()) for tag, model in self.models.items()]

    def concurrent_predict(self, df):
        with ThreadPoolExecutor() as executor:
            futures = [ (tag, executor.submit(model.predict, df)) for tag, model in self.models.items()]
            predictions = []
            for tag, future in futures:
                result = (tag, future.result().tolist() )
                predictions.append( result )
        return predictions
