import pandas as pd
from custom_model import RoutingModel
import json
from io import BytesIO, StringIO
import logging
import time
from datarobot_mlops.mlops import MLOps
import os


## this will be an unstructured model exposing the following hooks
## the following hooks are unstructured predict pecific
# init, load_model, score_unstructured
## score_unstructued takes arbitrary input and returns arbitrary output
logging.basicConfig(level=logging.INFO)
logging.basicConfig(
                format="{} - %(levelname)s - %(asctime)s - %(message)s".format("debug-loggers"),
        )
logger = logging.getLogger(__name__)
try: 
    mlops = MLOps().init()
except Exception as e:
    print(e)
    mlops = None

def init(**kwargs):
    """
    This hook can be implemented to adjust logic in the training and scoring mode.
    init is called once the code is started.

    :param kwargs: additional keyword arguments to the function.
    code_dir - code folder passed in --code_dir argument
    """
    pass

def load_model(input_dir):
    """
    This hook can be implemented to adjust logic in the scoring mode.

    load_model hook provides a way to implement model loading your self.
    This function should return an object that represents your model. This object will
    be passed to the predict hook for performing predictions.
    This hook can be used to load supported models if your model has multiple artifacts, or
    for loading models that drum does not natively support

    :param input_dir: the directory to load serialized models from
    :returns: Object containing the model - the predict hook will get this object as a parameter
    """
    # Returning a string with value "dummy" as the model.
    return RoutingModel(input_dir)

def score_unstructured(model, data, query, **kwargs):
    if kwargs["mimetype"] in ["application/text", "text/csv"]:
        so = StringIO(data.decode())
        df = pd.read_csv(so)
    elif kwargs["mimetype"] == "application/json":
        j = json.loads(data)
        df = pd.DataFrame(j)
    else:
        logger.warning(f"recieved mimetype {kwargs['mimetype']} is not one of application/text, text/csv, application/json")
        return json.dumps({"message": f"{kwargs['mimetype']} recieved, but model does not know how to handle"})
    start = time.time() 
    ## concurrent predictions, or 
    # preds = model.futures_predict(df)
    ## sequential predictions
    preds = dict( model.predict(df))
    end = time.time()
    if mlops:
      mlops.report_predictions_data(features_df = df, deployment_id = os.environ.get("DEPLOYMENT_ID"),model_id = os.environ.get("MODEL_ID") )
      mlops.report_deployment_stats(df.shape[0], (end - start)*1000, deployment_id = os.environ.get("DEPLOYMENT_ID"),model_id = os.environ.get("MODEL_ID"))
      for model_config in model.routing_config:
          tag = model_config["tag"]
          dep_id = model_config["deployment_id"]
          model_id = model_config["model_id"] 
          print(f"reporting prediction data for tag: {tag} deployment id:{dep_id} model package id: {model_id}")
          mlops.report_deployment_stats(df.shape[0], (end - start)*1000 / 19, deployment_id = dep_id, model_id = model_id)
          mlops.report_predictions_data(predictions = preds[tag], deployment_id = dep_id, model_id = model_id)
    return json.dumps(preds)