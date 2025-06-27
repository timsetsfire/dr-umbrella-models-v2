import logging
import datarobot as dr
from pathlib import Path
import yaml
import time
import argparse
import json
import requests
import sys
import pandas as pd 
import numpy as np 
import datetime
from requests_futures.sessions import FuturesSession
import asyncio
import os

client = dr.Client() 

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format='%(asctime)s %(filename)s:%(lineno)d %(levelname)s %(message)s',
)

logger = logging.getLogger(__name__)
client = dr.Client() 

features_to_track = ["age", "bmi", "region"]

def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__, usage='python %(prog)s'
    )
    parser.add_argument('--deployment-conf', help='existing path or where to write deployment configuration yaml.')
    parser.add_argument('--training-dataset-id', help='prediction dataset id for dataset registered to datarobot', default = None)
    parser.add_argument('--training-dataset-path', help='training dataset csv (includes target and feature).  Cannot be used with training-dataset-id', default=None)
    return parser.parse_args()

def validate_model_conf(model_conf):
    """following the readme.md, just validating that the provide model confs have the required key value pairs"""
    required_keys = {"target_type", "name", "prediction_environment_id"}
    binary_target_required_keys = {"positive_class_label", "negative_class_label"}
    if "prediction_environment_id" not in model_conf:
        logger.error("no prediction_environment_id has been specified.  Please provide one")
        raise Exception("no prediction_environment_id has been specified.  Please provide one")
    if required_keys.issubset(model_conf.keys()):
        if model_conf.get("target_type").lower() == "binary":
            if binary_target_required_keys.issubset( model_conf.keys()):
                pass
            else:
                logger.error("invalid model config.  Missing positive_class_label and / or negative_class_label")
                raise Exception("invalid model config.  Missing positive_class_label and / or negative_class_label")
    else: 
        logger.error(f"invalid model config.  missign one of the followign keys {required_keys}")
        raise Exception(f"invalid model config.  missign one of the followign keys {required_keys}")
    return model_conf

def purge_old_dataset_version(dataset_id, limit=100):
    limit = limit - 1
    dataset_url = f"datasets/{dataset_id}/versions?limit={limit}"
    def helper(dataset_url): 
        dataset_versions = client.get(dataset_url).json()
        if "offset" in dataset_url:
            print("off set present, deleting old versions")
            for d in dataset_versions["data"]:
                print(f"datasets/{d['datasetId']}/versions/{d['versionId']}/")
                delete_req = client.delete(f"datasets/{d['datasetId']}/versions/{d['versionId']}/")
                print(delete_req)
        if next := dataset_versions.get("next"):
            print(next)
            print(dataset_url)
            query_parameters = next.split("/")[-1]
            next_url = os.path.join( dataset_url.split("?")[0], query_parameters)
            print(next_url)
            helper(next_url)
    helper(dataset_url)

def register_dataset(conf, training_dataset_id, training_dataset_path):
    if training_dataset_id is not None and training_dataset_path is not None:
        logger.info("when providing both training dataset id and training dataset path in config, it is assumed a new version should be registered.")
    elif training_dataset_id is None and training_dataset_path is None: 
        logger.error("you must provide either training-dataset-id OR training-dataset (path to csv)")
        raise Exception("you must provide either training-dataset-id OR training-dataset (path to csv)")  

    if training_dataset_id is None and training_dataset_path is not None:
        training_dataset_path = Path(training_dataset_path)
        logger.info("register training dataset")
        training_dataset = dr.Dataset.create_from_file(str(training_dataset_path))    
        logger.info("recording training dataset id to deployment config")
        conf["training_dataset_id"] = training_dataset.id   
    elif training_dataset_id is not None and training_dataset_path is None:
        logger.info("training dataset id has been provided and will be used to set drift baselines for deployments")
        conf["training_dataset_id"] = training_dataset_id
    elif training_dataset_id is not None and training_dataset_path is not None:
        logger.info("training dataset id and trainign data path are present, registering new version")
        # purge_old_dataset_version(training_dataset_id)
        training_dataset = dr.Dataset.create_version_from_file(training_dataset_id, str(training_dataset_path))
        conf["training_dataset_id"] = training_dataset.id
    else:
        raise Exception("expection occured since neither training dataset nor training dataset id were provided.")
    return conf

def create_external_model_version(conf):
    while True: 
        try:
            registered_model_id = conf.get("registered_model_id")
            name = conf.get("name")
            training_dataset_id = conf.get("training_dataset_id")
            target_name=conf.get("target_name")
            target_type= conf.get("target_type")
            description = conf.get("description")
            if registered_model_id:
                registered_model_name = None
                logger.info(f"{name}: registered model with id {registered_model_id} exists.  Adding a new version")
            else:
                logger.info(f"{name}: registered model does not exist.  creating new entry")
                ts = datetime.datetime.now()
                registered_model_name = f"external {name} {ts}"
            if conf.get("positive_class_label"):
                class_names = [conf.get("positive_class_label"), conf.get("negative_class_label")]
            elif conf.get("class_names"):
                class_names = conf.get("class_names")
            else:
                class_names = None
            ext_reg_model = dr.RegisteredModelVersion.create_for_external(
                name = name, 
                registered_model_id = registered_model_id,
                target = {"type": target_type, "name": target_name, "predictionThreshold": conf.get("prediction_threshold"), "classNames": class_names},
                datasets = {"trainingDataCatalogId": training_dataset_id}, 
                registered_model_name = registered_model_name,
                registered_model_description=description
            )
            conf["registered_model_id"] = ext_reg_model.registered_model_id 
            conf["registered_model_version_id"] = ext_reg_model.id 
            return conf
        except Exception as e:
            logger.info(e)
            try:
                message = e.json["message"]
                wait_for = int(message.split(" ")[-2])
            except Exception:
                logger.error("Unexpected error format. Aborting retry.")
                raise
            logger.info(f"Waiting for {wait_for} seconds before retrying to register external model.")
            time.sleep(wait_for)

def create_external_deployment(conf):
    name = conf["name"]
    description = conf.get("description")
    prediction_environment_id = conf.get("prediction_environment_id")
    if deployment_id := conf.get("deployment_id"):
        deployment = dr.Deployment.get(deployment_id)
        name = conf.get("name")
        logger.info(f"updated deployment {name} with new version")
        logger.info(f"{name}: refreshing deployment")
        validation_response = client.post(f"deployments/{deployment_id}/model/validation/", data = {"modelPackageId": conf["registered_model_version_id"]})
        if validation_response.status_code == 200:
            logger.info("model deployment validation passed")
        else:
            logger.error( validation_response.json())
        replacement_response = client.patch(f"deployments/{deployment_id}/model/", data = {"modelPackageId": conf["registered_model_version_id"], "reason": "SCHEDULED_REFRESH"})
        if replacement_response.status_code == 202:
            logger.info("deployment refresh successful")
        else:
            logger.error(f"something went wrong.  replacement response returned status code {replacement_response.status_code}")
        deployment = dr.Deployment.get(deployment_id)
    else:
        logger.info(f"{name}: deployment does not exists")
        logger.info(f"{name}: creating deployment")
        deployment = dr.Deployment.create_from_registered_model_version(
            model_package_id = conf["registered_model_version_id"],
            label= name,
            description=description,
            prediction_environment_id=prediction_environment_id
        )
    conf["deployment_id"] = deployment.id
    conf["url"] = f"https://app.datarobot.com/console-nextgen/deployments/{deployment.id}"
    return conf 

async def update_deployment_settings(conf, target_drift = True, feature_drift = True):
    features_to_track = conf.get("features_to_track", [])
    logger.info(f"features to track: {features_to_track}")
    if len(features_to_track) == 0:
        feature_selection = "auto"
    else:
        feature_selection = "manual"
    logger.info(f"feature selection for drift is {feature_selection}")
    def _create():
        deployment_id = conf.get("deployment_id")
        client = dr.Client()
        deployment = dr.Deployment.get(deployment_id)
        # # Enabling Accuracy
        logger.info("updating association id for accuracy tracking")
        deployment.update_association_id_settings(column_names=["ASSOCIATION_ID"], required_in_prediction_requests=False)
        # # Enabling Challenger
        logger.info("updating prediction data collection settings")
        deployment.update_predictions_data_collection_settings(enabled=True) 
        payload = {"targetDrift":{"enabled":target_drift},
                                         "featureDrift":{"enabled":feature_drift,"featureSelection":feature_selection,"trackedFeatures":features_to_track}}
        logger.info(f"feature drift settings payload: {payload}")
        dep_patch = client.patch(f"deployments/{deployment_id}/settings/", 
                                 data = payload)
        logger.info(f'{conf["name"]}: updated accuracy settings and drift settings on deployment monitorin')
        logger.info(dep_patch.status_code)
        logger.info("update complete")
    return await asyncio.to_thread(_create)

async def main():
    args = parse_args()
    deployment_conf_path = args.deployment_conf
    training_dataset_path = args.training_dataset_path 
    training_dataset_id = args.training_dataset_id
    logger.info(args)

    if deployment_conf_path is not None:
        if Path(deployment_conf_path).exists():
            logger.info(f"using existing deployment conf at {deployment_conf_path}")
            with open(deployment_conf_path, "r") as f:
                master_conf = yaml.load(f, Loader = yaml.SafeLoader)
        else:
            logger.error("deployment configuration yaml does not exists")
            raise Exception("deployment configuration yaml does not exist") 
    else:
        logger.error("deployment configuration yaml does not exists")
        raise Exception("deployment configuration yaml does not exist")
    
    if isinstance(master_conf, dict):
        deployment_confs = master_conf["deployments"]
    elif isinstance(master_conf, list):
        deployment_confs = master_conf

       
    deployment_conf = [ validate_model_conf(conf) for conf in deployment_confs]                  
    deployment_conf = [ register_dataset(conf, training_dataset_id, training_dataset_path) for conf in deployment_confs]
    deployment_conf = [ create_external_model_version(conf) for conf in deployment_confs]
    deployment_conf = [ create_external_deployment(conf) for conf in deployment_confs]
    await asyncio.gather( *[update_deployment_settings(conf, True, True) for conf in deployment_confs])

    if isinstance(master_conf, list):
        final_out = {"deployments": deployment_conf}
    else:
        final_out = master_conf 
        final_out["deployments"] = deployment_conf

    logger.info(f"updated deployment conf at {deployment_conf_path}")
    with open(str(deployment_conf_path), "w") as f:
        f.write(yaml.dump(final_out))
    logger.info("deployments have been updated!!")

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))