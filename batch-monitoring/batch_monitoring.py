import logging
import datarobot as dr
from pathlib import Path
import yaml
import time
import argparse
import json
import requests
import sys
import os
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format='%(asctime)s %(filename)s:%(lineno)d %(levelname)s %(message)s',
)
logger = logging.getLogger(__name__)
client = dr.Client() 

## check number of versions and delete old ones
## datarobot  has a soft 100 version limit on registered datasets.  
## thie function is will delete old version (fifo) based on limit arg
def purge_old_dataset_version(dataset_id, limit=100):
    limit = limit - 1
    dataset_url = f"datasets/{dataset_id}/versions?limit={limit}"
    total_count = client.get(dataset_url).json()['totalCount']
    logger.info(f"number of versions of prediction dataset: {total_count}")
    logger.info(f"number of versions allowed: {limit + 1}")
    deletion = total_count - limit
    if deletion > 0:
        logger.info(f"number of versions to delete before registering new version (FIFO): { deletion }")
    def helper(dataset_url): 
        dataset_versions = client.get(dataset_url).json()
        if "offset" in dataset_url:
            logger.info("deleting old versions")
            for d in dataset_versions["data"]:
                logger.info(f"deleting datasets/{d['datasetId']}/versions/{d['versionId']}/")
                delete_req = client.delete(f"datasets/{d['datasetId']}/versions/{d['versionId']}/")
                if delete_req.status_code >= 400:
                    logger.error(delete_req.json())
                    raise Exception(delete_req.json())
                else:
                    logger.info("dataset version deleted successfully")
        if next := dataset_versions.get("next"):
            query_parameters = next.split("/")[-1]
            next_url = os.path.join( dataset_url.split("?")[0], query_parameters)
            helper(next_url)
    helper(dataset_url)

def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__, usage='python %(prog)s <deployment_conf.yaml>'
    )
    parser.add_argument('--deployment-conf', help='deployment configuration yaml.')
    parser.add_argument('--prediction-dataset', help='prediction dataset csv (includes predictions and inputs)')
    parser.add_argument('--input-dataset', help='input dataset csv (includes features for the mondel)')
    return parser.parse_args()

def main():
    args = parse_args()
    deployment_conf_path = args.deployment_conf
    prediction_dataset_path = args.prediction_dataset
    input_dataset_path = args.input_dataset
    deployment_conf_path = Path(deployment_conf_path)
    prediction_dataset_path = Path(prediction_dataset_path)
    
    if deployment_conf_path.exists() and prediction_dataset_path.exists():
        with open(deployment_conf_path, "r") as f:
            deployment_conf = yaml.load(f, Loader = yaml.SafeLoader)
        MONITOR = True
        pred_df = pd.read_csv(str(prediction_dataset_path))
        if Path(input_dataset_path).exists():
            logger.info(f"input dataset is available at {input_dataset_path}")
            input_df = pd.read_csv(str(input_dataset_path))
            monitoring_df = pred_df.join(input_df)
        else:
            monitoring_df = pred_df.copy()
            logger.warning(f"{str(input_dataset_path)} does NOT exist or was not provided.")
            logger.warning(f"no feature drift will be monitoring for this run")
        expected_pred_columns = [d["prediction_column"] for d in deployment_conf["deployments"]]
        received_pred_columns = [ p for p in monitoring_df.columns if 'ADJ_PRED_RENTAL_DAYS_' in p]
        num_missing = 0
        invalid_pred_columns = []
        for rpc in received_pred_columns:
            if rpc.replace(".", "_") not in expected_pred_columns:
                logging.error(f"after sanitizing column names, invalid column name observed: {rpc}")
                invalid_pred_columns.append(rpc)
                num_missing+=1
        if num_missing > 0:
            raise Exception(f"after sanitizing invalid prediction columns were observed: {invalid_pred_columns}.  Sanitization involves replace `.` with `_`.")           
    else:
        MONITOR = False
        raise Exception("Deployment configuration or prediction dataset does not exist!! Monitoring not available")

    if MONITOR: 
        if prediction_dataset_id := deployment_conf.get("prediction_dataset_id"):
            logger.info("prediction dataset id present")
            purge_old_dataset_version(prediction_dataset_id)
            logger.info("registering new version")                        
            # prediction_dataset = dr.Dataset.create_version_from_file(prediction_dataset_id, str(prediction_dataset_path))
            prediction_dataset = dr.Dataset.create_version_from_in_memory_data(prediction_dataset_id, monitoring_df)
        else:
            logger.info("register prediction dataset")
            # prediction_dataset = dr.Dataset.create_from_file(str(prediction_dataset_path), fname = f"Rental Calc Prediction Data")    
            prediction_dataset = dr.Dataset.create_from_in_memory_data(monitoring_df, fname = f"Rental Calc Prediction Data") 
            logger.info("recording prediction dataset id to deployment config")
            deployment_conf["prediction_dataset_id"] = prediction_dataset.id     
        
        logger.info("done with prediction dataset registration")
    
        for model in deployment_conf.get("deployments"):
            if model.get("batch_monitoring_job_id"):
                pass
            else: 
                logger.info(f"creating monitoring job for {model['prediction_column']}")
                monitoring_job_payload = {
                    "deploymentId":model["deployment_id"],
                    "monitoringAggregation": None,
                    "intakeSettings":{"type":"dataset","datasetId":deployment_conf["prediction_dataset_id"]},
                    "name":f"Rental Calc Monitoring {model['prediction_column']}",
                    "enabled":False,
                    "monitoringColumns": {"predictionsColumns":model["prediction_column"]}}
                monitoring_job_response = client.post("batchMonitoringJobDefinitions/", data = monitoring_job_payload)
                monitoring_job_response.raise_for_status()
                payload_patch = {"monitoringAggregation":None}
                batch_monitoring_job_id = monitoring_job_response.json()["id"]
                patch_response = requests.patch( f"{client.endpoint}/batchMonitoringJobDefinitions/{batch_monitoring_job_id}/", 
                              headers = { 
                                  "Authorization": f"Bearer {client.token}",
                                  'Content-Type': "application/json" 
                              },
                              data = json.dumps(payload_patch))
                model["batch_monitoring_job_id"] = batch_monitoring_job_id
                logger.info("recording batch monitoring job id to deployment config")
            
        logger.info(f"writing deployemnt config to {str(deployment_conf_path)}")
        with open(str(deployment_conf_path), "w") as f:
            f.write(yaml.dump(deployment_conf))

        batch_jobs = []
        for model in deployment_conf["deployments"]:
            logger.info(f"running monitoring job for {model['prediction_column']}")
            job_run_payload = {"jobDefinitionId":model["batch_monitoring_job_id"]}
            job_run_response = client.post("batchJobs/fromJobDefinition/", data = job_run_payload)
            job_run_response.raise_for_status()
            batch_jobs.append(job_run_response.json())
        logger.info(f"{len(batch_jobs)} monitoring jobs in process!")
        
        for job in batch_jobs:
            job = client.get(f"batchJobs/{job['id']}").json()
            while job["status"] in ["INITIALIZING", "RUNNING"]:
                job = client.get(f"batchJobs/{job['id']}").json()
                time.sleep(5)
            if job["status"] == "COMPLETED":
                logger.info(job['batchMonitoringJobDefinition']['name'])
                logger.info(job["logs"][-1])
            elif job["status"] in ["ABORTED", "FAILED"]:
                logger.error( job["logs"][-1])
        logger.info("removing prediction dataset from disk")
        Path(prediction_dataset_path).unlink()
        if input_dataset_path:
            logger.info("removing input dataset from disk")
            Path(input_dataset_path).unlink()
        
    else:
        logger.info("monitoring was not enabled -> either missing deployment conf yaml or prediction datasets")

if __name__ == "__main__":
    sys.exit(main())
## this will maintain the 99 latest versions of a dataset, so plus the new version from the next cell, that will put us at 100 versions
