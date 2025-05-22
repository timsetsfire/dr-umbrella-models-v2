"""
Usage:
    python datarobot-predict.py <input-file.csv>
 
This example uses the requests library which you can install with:
    pip install requests
We highly recommend that you update SSL certificates with:
    pip install -U urllib3[secure] certifi
"""
import sys
import json
import requests

MAX_PREDICTION_FILE_SIZE_BYTES = 52428800  # 50 MB


class DataRobotPredictionError(Exception):
    """Raised if there are issues getting predictions from DataRobot"""


def make_datarobot_deployment_url_payload(
    deployment_id, api_url, api_key, datarobot_key=None, passthrough_columns=None
):
    # Set HTTP headers. The charset should match the contents of the file.
    headers = {
        "Content-Type": "text/plain; charset=UTF-8",
        # 'Content-Type': 'application/json; charset=UTF-8',
        "Authorization": "Bearer {}".format(api_key),
        # "DataRobot-Key": datarobot_key,
        'Accept': "text/csv"
    }

    url = api_url 

    params = {
        # If explanations are required, uncomment the line below
        # 'maxExplanations': 3,
        # 'thresholdHigh': 0.5,
        # 'thresholdLow': 0.15,
        # Uncomment this for Prediction Warnings, if enabled for your deployment.
        # 'predictionWarningEnabled': 'true',
        "passthroughColumns": passthrough_columns
    }
    out = {"url": url, "params": params, "headers": headers}
    return out
