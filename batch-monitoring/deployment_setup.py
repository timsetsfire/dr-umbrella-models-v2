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

client = dr.Client() 

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format='%(asctime)s %(filename)s:%(lineno)d %(levelname)s %(message)s',
)
logger = logging.getLogger(__name__)
client = dr.Client() 

def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__, usage='python %(prog)s'
    )
    parser.add_argument('--deployment-conf', help='existing path or where to write deployment configuration yaml.')
    parser.add_argument('--training-dataset-id', help='prediction dataset id for dataset registered to datarobot')
    parser.add_argument('--training-dataset', help='training dataset csv (includes target and feature).  Cannot be used with training-dataset-id')
    return parser.parse_args()

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

def main():
    args = parse_args()
    deployment_conf_path = args.deployment_conf
    training_dataset_id = args.training_dataset_id 
    training_dataset_path = args.training_dataset
    logger.info(args)

    if deployment_conf_path is not None:
        if Path(deployment_conf_path).exists():
            logger.info(f"using existing deployment conf at {deployment_conf_path}")
            with open(deployment_conf_path, "r") as f:
                deployment_conf = yaml.load(f, Loader = yaml.SafeLoader)
        else: 
            logger.warning(f"{deployment_conf_path} does not exists")
            deployment_conf_path = "./deployment_conf.yaml"
            logger.info(f"{str(deployment_conf_path)} does not exists, making a new one and will write to {str(deployment_conf_path)}")
            quantiles = np.linspace(0.05, 0.95, 19)
            quantiles = [np.round(x,3) for x in quantiles]
            deployment_conf = dict(deployments = [{"prediction_column": f"ADJ_PRED_RENTAL_DAYS_Q_{q}".replace(".", "_")} for q in quantiles])
    else:
        deployment_conf_path = "./deployment_conf.yaml"
        logger.info("deployment_conf.yaml does not exists, making a new one and will write to ./deployment_conf.yaml")
        quantiles = np.linspace(0.05, 0.95, 19)
        quantiles = [np.round(x,3) for x in quantiles]
        deployment_conf = dict(deployments = [{"prediction_column": f"ADJ_PRED_RENTAL_DAYS_Q_{q}".replace(".", "_")} for q in quantiles])
            

    if training_dataset_id is not None and training_dataset_path is not None:
        logger.error("you can only provide training-dataset-id OR training-dataset.  Not both")
        raise Exception("you can only provide training-dataset-id OR training-dataset.  Not both")
    elif training_dataset_id is None and training_dataset_path is None: 
        logger.error("you must provide either training-dataset-id OR training-dataset (path to csv)")
        raise Exception("you must provide either training-dataset-id OR training-dataset (path to csv)")  

    if training_dataset_id is None and training_dataset_path is not None:
        training_dataset_path = Path(training_dataset_path)
        if existing_training_dataset_id := deployment_conf.get("training_dataset_id"):
            logger.info("training dataset id present in conf, registering new version")
            purge_old_dataset_version(existing_training_dataset_id)
            training_dataset = dr.Dataset.create_version_from_file(existing_training_dataset_id, str(training_dataset_path))
        else:
            logger.info("register training dataset")
            training_dataset = dr.Dataset.create_from_file(str(training_dataset_path))    
            logger.info("recording training dataset id to deployment config")
            deployment_conf["training_dataset_id"] = training_dataset.id   
    elif training_dataset_id is not None and training_dataset_path is None:
        logger.info("training dataset id has been provided and will be used to set drift baselines for deployments")
        deployment_conf["training_dataset_id"] = training_dataset_id
        training_dataset = dr.Dataset.get(training_dataset_id)
    else:
        raise Exception("expection occured since training dataset is not none and training dataset path is not none or both are none")
        
    if pred_env_id := deployment_conf.get("prediction_environment_id"):
        logger.info("prediction environment exists")
        prediction_environment = dr.PredictionEnvironment.get(pred_env_id)
    else:
        logger.info("prediction environment doesn't exist, creating one")
        prediction_environment = dr.PredictionEnvironment.create(name = "Rental Calc External Prediction Environment", 
                                    platform = dr.enums.PredictionEnvironmentPlatform.OTHER,
                                    description = "DataRobot Codespace Running Scheduled Notebooks")
        logger.info(prediction_environment.__dict__)
        deployment_conf["prediction_environment_id"] = prediction_environment.id

    registerd_external_models = []
    for model in deployment_conf["deployments"]:
        registered_model_id = model.get("registered_model_id")
        quantile = model["prediction_column"]
        if registered_model_id:
            registered_model_name = None
            logger.info(f"{model['prediction_column']}: registered model with id {registered_model_id} exists.  Adding a new version")
        else:
            logger.info(f"{model['prediction_column']}: registered model does not exist.  creating new entry")
            ts = datetime.datetime.now()
            registered_model_name = f"external {quantile} {ts}"
        ext_reg_model = dr.RegisteredModelVersion.create_for_external(
            name = quantile, 
            registered_model_id = registered_model_id,
            target = {"type": "Regression", "name": "charges"},
            datasets = {"trainingDataCatalogId": deployment_conf["training_dataset_id"]}, 
            registered_model_name = registered_model_name,
            registered_model_description=f"{quantile} model that has been packaged with umbrella model"
        )
        model["registered_model_id"] = ext_reg_model.registered_model_id 
        model["registered_model_version_id"] = ext_reg_model.id 
        registerd_external_models.append(ext_reg_model)
        ## need to pause jsut to not choke the server
        time.sleep(5)

    deployments = []
    for model in deployment_conf["deployments"]:
        if deployment_id := model.get("deployment_id"):
            logger.info(f"updated deployment for  {model['prediction_column']} with new version")
            logger.info(f"{model['prediction_column']}: refreshing deployment")
            validation_response = client.post(f"deployments/{deployment_id}/model/validation/", data = {"modelPackageId": model["registered_model_version_id"]})
            if validation_response.status_code == 200:
                logger.info("model deployment validation passed")
            else:
                logger.error( validation_response.json())
            replacement_response = client.patch(f"deployments/{deployment_id}/model/", data = {"modelPackageId": model["registered_model_version_id"], "reason": "SCHEDULED_REFRESH"})
            if replacement_response.status_code == 202:
                logger.info("deployment refresh successful")
            else:
                logger.error(f"something went wrong.  replacement response returned status code {replacement_response.status_code}")
            deployment = dr.Deployment.get(deployment_id)
        else:
            logger.info(f"{model['prediction_column']}: deployment does not exists")
            logger.info(f"{model['prediction_column']}: creating deployment")
            deployment = dr.Deployment.create_from_registered_model_version(
                model_package_id = model["registered_model_version_id"],
                label= model["prediction_column"],
                description=f"external model deployment for {model['prediction_column']}",
                prediction_environment_id=prediction_environment.id
            )
        
        # # Enabling Accuracy
        logger.info(f"{model['prediction_column']}: updated drift and accuracy settings on deployment monitorin")
        deployment.update_association_id_settings(column_names=["ASSOCIATION_ID"], required_in_prediction_requests=False)
        # # Enabling Challenger
        deployment.update_predictions_data_collection_settings(enabled=True)
        # ## enable data drift and prediction trakcign (really really slow)
        # deployment.update_drift_tracking_settings(target_drift_enabled=True, feature_drift_enabled=True)
        # ## direct patch of deployment -> seems to go quicker
        dep_patch = client.patch(f"deployments/{deployment.id}/settings/", data = {"targetDrift":{"enabled":True},"featureDrift":{"enabled":True,"featureSelection":"auto","trackedFeatures":[]}})
        model["deployment_id"] = deployment.id
        model["target_type"] = "Regression"
        model["url"] = f"https://app.datarobot.com/console-nextgen/deployments/{deployment.id}"
        deployments.append(deployment)

    logger.info(f"updated deployment conf at {deployment_conf_path}")
    with open(str(deployment_conf_path), "w") as f:
        f.write(yaml.dump(deployment_conf))
    logger.info("deployments have been updated!!")

if __name__ == "__main__":
    sys.exit(main())