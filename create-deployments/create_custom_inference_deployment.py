import sys
import logging
import subprocess
import datarobot as dr 
import asyncio
from datetime import datetime
import yaml
import argparse
import os
from pathlib import Path

client = dr.Client()

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format='%(asctime)s %(filename)s:%(lineno)d %(levelname)s %(message)s',
)
logger = logging.getLogger(__name__)

def validate_model_conf(model_conf):
    """following the readme.md, just validating that the provide model confs have the required key value pairs"""
    required_keys = {"artifact_folder", "target_type", "drum_test_data_path", "name"}
    binary_target_required_keys = {"positive_class_label", "negative_class_label"}
    if "training_dataset_path" or "training_dataset_id" in model_conf.keys():
        pass
    else:
        logger.error("no training dataset info is available.  Please either provide training_dataset_path or training_dataset_id")
        raise Exception("no training dataset info is available.  Please either provide training_dataset_path or training_dataset_id")
    if "environment_id" not in model_conf:
        logger.error("no environment_id has been specified.  Please provide one")
        raise Exception("no environment_id has been specified.  Please provide one")
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

async def drum_test(conf):
    target_type = conf["target_type"].lower() 
    artifact_folder = conf["artifact_folder"] 
    drum_test_data_path = conf["drum_test_data_path"]
    if target_type == "binary":
        positve_class_label = conf.get("positive_class_label", "1")
        negative_class_label = conf.get("negative_class_label", "0")
        cmd = [
            "drum", "score",
            "--code-dir", artifact_folder,
            "--target-type", target_type,
            "--input", drum_test_data_path,
            "--positive-class-label", positve_class_label,
            "--negative-class-label", negative_class_label,
        ]
    elif target_type == "regression":
        cmd = [
            "drum", "score",
            "--code-dir", artifact_folder,
            "--target-type", target_type,
            "--input", drum_test_data_path,
        ] 
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    logger.debug(stdout)
    if process.returncode != 0:
        logger.error("drum failed with non-zero return code")
        logger.error(stdout.decode())
        logger.error(stderr.decode())
        raise Exception(f"drum test failed with return code {process.returncode}")
    logger.info(f"drum test completed successfully for {conf['name']}")
    conf["drum_test_passed"] = True
    return conf 

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

async def register_dataset(conf, training_dataset_id, training_dataset_path):
    def _create(training_dataset_id, training_dataset_path): 
        client = dr.Client()
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
            logger.info("training dataset id and trainign data path have been provided, registering new version")
            # purge_old_dataset_version(training_dataset_id)
            conf["training_dataset_id"] = training_dataset_id 
            training_dataset = dr.Dataset.create_version_from_file(training_dataset_id, str(training_dataset_path))
        else:
            raise Exception("expection occured since neither training dataset nor training dataset id were provided.")
        return conf
    return await asyncio.to_thread(_create, training_dataset_id, training_dataset_path)  

async def create_custom_model_version(conf):
    def _create():
        client = dr.Client()
        custom_model_id = conf.get("custom_model_id") 
        artifact_folder = conf.get("artifact_folder")
        name = conf.get("name")
        training_dataset_id = conf.get("training_dataset_id")
        environment_id = conf.get("environment_id")

        if custom_model_id is None: 
            logger.info(f"{name}: creating new custom inference model")
            cm = dr.CustomInferenceModel.create(
                name, 
                target_name=conf.get("target_name"),
                target_type= conf.get("target_type"), 
                positive_class_label = conf.get("positive_class_label"),
                negative_class_label = conf.get("negative_class_label"), 
                class_labels = conf.get("class_labels")
            )
            custom_model_id = cm.id
        else: 
            logger.info(f"{name}: creating new version of custom inference model")
            cm = dr.CustomInferenceModel.get(custom_model_id)
        cmv = dr.CustomModelVersion.create_clean(cm.id, 
                                                base_environment_id = environment_id,
                                                folder_path = artifact_folder, 
                                                training_dataset_id=training_dataset_id
                                                )
        conf["custom_model_id"] = cm.id 
        conf["custom_model_version_id"] = cmv.id 
        conf["includes_requirements"] = any( item.file_name=="requirements.txt" for item in cmv.items)
        return conf
    return await asyncio.to_thread(_create)    


async def build_custom_model_environment(conf):
    def _create():
        client = dr.Client()
        if conf["includes_requirements"]:
            logger.info("building custom model environment")
            cm_id = conf.get("custom_model_id")
            cmv_id = conf.get("custom_model_version_id")
            url = f"customModels/{cm_id}/versions/{cmv_id}/dependencyBuild/"
            try:
                build_req = client.post(url)
                build_info = dr.CustomModelVersionDependencyBuild.get_build_info(cm_id, cmv_id)
            # build = dr.CustomModelVersionDependencyBuild.start_build(cm.id, cmv.id, max_wait = 1200)
            except Exception as e: 
                logger.warning(e)
                build_info = None
        else:
            logger.info("no requirements.txt found, skipping build")
            build_info = None
        return conf, build_info
    return await asyncio.to_thread(_create)

async def wait_for_custom_model_environment(conf, build_info):
    def _create():
        client = dr.Client()
        if build_info is None:
            return conf
        while build_info.build_status in  ["submitted", "processing"]:
            build_info.refresh()
        if build_info.build_status != "success":
            logger.info(build_info)
        else:
            logger.info(build_info)
        conf["environment_build_status"] = build_info.build_status
        return conf
    return await asyncio.to_thread(_create)

async def test_custom_model(conf):
    def _create():
        client = dr.Client()
        cm_id = conf.get("custom_model_id")
        cmv_id = conf.get("custom_model_version_id")
        test_dataset_id = conf.get("test_dataset_id", conf.get("training_dataset_id"))
        if test_dataset_id is None:
            logger.warning("not test dataset is provided. for testing custom model in datarobot.  Skipping test")
            return conf, None
        else:
            custom_model_test = dr.CustomModelTest.create(
                cm_id, 
                cmv_id, 
                dataset_id = test_dataset_id, 
                network_egress_policy = dr.enums.NETWORK_EGRESS_POLICY.PUBLIC) 
            conf["custom_model_test_status"] = custom_model_test.overall_status
            if custom_model_test.overall_status != "succeeded":
                logger.warning(f"custom model test had status {custom_model_test.overall_status}")
                logger.warning(custom_model_test.detailed_status)
                conf["custom_model_test_details"] = custom_model_test.detailed_status
            return conf
    return await asyncio.to_thread(_create)
    

async def register_custom_model(conf):
    async def try_register():
        def _create():
            client = dr.Client()
            cmv_id = conf.get("custom_model_version_id")
            registered_model_id = conf.get("registered_model_id")
            registered_model_name = f"{conf.get('name')} {datetime.now()}" if registered_model_id is None else None
            return dr.RegisteredModelVersion.create_for_custom_model_version(
                custom_model_version_id=cmv_id,
                name=conf.get("name"),
                registered_model_name=registered_model_name,
                description=conf.get("name"),
                registered_model_id=registered_model_id
            )

        return await asyncio.to_thread(_create)
    while True:
        try:
            logger.info("registering model - this could take a while")
            registered_model_version = await try_register()
            conf["registered_model_version_id"] = registered_model_version.id
            conf["registered_model_id"] = registered_model_version.registered_model_id
            return conf
        except Exception as e:
            logger.info(e)
            try:
                message = e.json["message"]
                wait_for = int(message.split(" ")[-2])
            except Exception:
                logger.error("Unexpected error format. Aborting retry.")
                raise
            logger.info(f"Waiting for {wait_for} seconds before retrying to register custom model.")
            await asyncio.sleep(wait_for)

async def wait_for_model_package_build(conf):
    def _create():
        client = dr.Client()
        rm = dr.RegisteredModel.get(conf["registered_model_id"])
        rmv = rm.get_version(conf["registered_model_version_id"])
        while rmv.build_status == "inProgress":
            rmv = rm.get_version(rmv.id)
        if rmv.build_status == "complete":
            logger.info("registered model model package build is complete")
        else:
            logger.error(f"something happened during model package build: {rmv.build_status}")
        return conf
    return await asyncio.to_thread(_create)

async def create_deployment(conf):
    def _create():
        client = dr.Client()
        prediction_environment_id = conf.get("prediction_environment_id")
        registered_model_version_id = conf["registered_model_version_id"]
        if deployment_id := conf.get("deployment_id"):
            logger.info(f"updated deployment for  with new version")
            validation_response = client.post(f"deployments/{deployment_id}/model/validation/", data = {"modelPackageId": registered_model_version_id})
            if validation_response.status_code == 200:
                logger.info("model deployment validation passed")
            else:
                logger.error( validation_response.json())
            replacement_response = client.patch(f"deployments/{deployment_id}/model/", data = {"modelPackageId": registered_model_version_id, "reason": "SCHEDULED_REFRESH"})
            if replacement_response.status_code == 202:
                logger.info("deployment refresh successful")
            else:
                logger.error(f"something went wrong.  replacement response returned status code {replacement_response.status_code}")
            deployment = dr.Deployment.get(deployment_id)
        else:
            logger.info(f"deployment does not exists")
            logger.info(f"creating deployment")
            deployment = dr.Deployment.create_from_registered_model_version(
                model_package_id = registered_model_version_id,
                label= conf["name"],
                description=f"model deployment for { conf['name']}",
                prediction_environment_id=prediction_environment_id,
                max_wait = 3600
            )
            conf["deployment_id"] = deployment.id
            conf["prediction_environment_id"] = prediction_environment_id
        if deployment.prediction_environment["platform"] == "datarobotServerless":
            realtime_endpoint = f"https://app.datarobot.com/api/v2/deployments/{deployment.id}/predictions"
        else: 
            realtime_endpoint = os.path.join(deployment.prediction_environmment["name"], "predApi/v1.0/deployments", deployment.id, "predictions")
        conf["realtime_endpoint"] = realtime_endpoint
        return conf
    return await asyncio.to_thread(_create)

async def update_deployment_settings(conf):
    features_to_track = conf.get("features_to_track", [])
    logger.info(f"features to track: {features_to_track}")
    if len(features_to_track) == 0:
        feature_selection = "auto"
    else:
        feature_selection = "manual"
    logger.info(f"feature selection for drift is {feature_selection}")
    def _create():
        client = dr.Client()
        deployment = dr.Deployment.get(conf["deployment_id"])
        deployment.update_association_id_settings(["ASSOCIATION_ID"], required_in_prediction_requests=False)
        deployment.update_predictions_data_collection_settings(enabled=True) 
        # deployment.update_drift_tracking_settings(target_drift_enabled=True, feature_drift_enabled=True)
        payload = {"targetDrift":{"enabled":True},
                                         "featureDrift":{"enabled":True,"featureSelection":feature_selection,"trackedFeatures":features_to_track}}
        logger.info(f"feature drift settings payload: {payload}")
        dep_patch = client.patch(f"deployments/{deployment.id}/settings/", 
                                 data = payload)
        return conf
    return await asyncio.to_thread(_create)

def write_model_confs(model_confs, output_path = "./deployment_conf.yaml"):
    logger.info(f"writing model confs to disk as {output_path}")
    with open(output_path,"w") as f:
        f.write(yaml.dump(model_confs))

def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__, usage='python %(prog)s'
    )
    parser.add_argument('--deployment-conf', help='path to deployment config yaml.  This must be provided.')
    parser.add_argument('--training-dataset-id', help='prediction dataset id for dataset registered to datarobot', default = None)
    parser.add_argument('--training-dataset-path', help='training dataset csv (includes target and feature).  Cannot be used with training-dataset-id', default=None)

    return parser.parse_args()

async def main(): 
    args = parse_args()
    deployment_confs_path = args.deployment_conf
    training_dataset_path = args.training_dataset_path 
    training_dataset_id = args.training_dataset_id

    with open(deployment_confs_path, "r") as f:
        master_conf = yaml.load(f, Loader = yaml.SafeLoader)

    if isinstance(master_conf, dict):
        model_confs = master_conf["deployments"]
    elif isinstance(master_conf, list):
        model_confs = master_conf
    
    model_confs = [ validate_model_conf(conf) for conf in model_confs]
    model_confs = await asyncio.gather( *[drum_test(conf) for conf in model_confs])
    model_confs = await asyncio.gather( *[register_dataset(conf, training_dataset_id, training_dataset_path) for conf in model_confs])
    model_confs = await asyncio.gather( *[create_custom_model_version(conf) for conf in model_confs])
    model_confs = await asyncio.gather( *[build_custom_model_environment(conf) for conf in model_confs])
    model_confs = await asyncio.gather( *[wait_for_custom_model_environment(conf, build_info) for conf, build_info in model_confs])
    model_confs = await asyncio.gather( *[register_custom_model(conf) for conf in model_confs])    
    model_confs = await asyncio.gather( *[wait_for_model_package_build(conf) for conf in model_confs])  
    model_confs = await asyncio.gather( *[create_deployment(conf) for conf in model_confs])   
    logger.info("success!!")  

    if isinstance(master_conf, list):
        final_out = {"deployments": model_confs}
    else:
        final_out = master_conf 
        final_out["deployments"] = model_confs

    write_model_confs(final_out, deployment_confs_path)

if __name__ == "__main__":
    asyncio.run(main())