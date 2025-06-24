# REQUIRES TESTING

## `create_external_deployments.py`

Currently only supported with structured regression and structured binary classification problems.  


This script will 
* registerd training dataset, or register a new version of the training datasets 
* register the model as an external model 
* "deploy" the model as an external deployment.  This just means that the model lives elsewhere for making predictions, but we will use datarobot console for monitoring the model: drift, accuracy, etc.

The script depends on a config yaml.  You will be expected to provide this configuratuion.  The input yaml contains a list of external models that should be "deployed" or have been "deployed" to datarobot.  An example entry in this list is as follows.  

```
- name: quantile-0.05 external test deployment
  prediction_environment_id: 68517d02a038d3c56d0bbb40
  target_name: charges
  target_type: Regression
  training_data_path: data/training_data_v2.csv
  training_dataset_id: 68404e7745fc9db4b112d002
```

You will be expected to provide the config, and you will be required to complete the following within each entry of the config.  
* `name` - name of the model as it will be registred in DataRobot -> MUST BE UNIQUE
* `prediction_environment_id`
* `target_name` - name of the target variable as it is in your training datasets
* `target_type` - either `Regression` of `Binary`

You get the `prediction_environment_id` from the Console -> Prediction Environments within the datarobot ui.  You need to ensure that the prediction environment is a "Other" Platform and it is Self-Managed.  

Concerning `training_dataset_id` and `training_dataset_path`, you must provide one of the following setups in the conf
* `training_dataset_id` and `training_dataset_path` - this registers a new version of the dataset with datarobot based on the file located at `training_dataset_path`.  
* `training_dataset_id` - this will use a previosuly registered dataset as the training dataset for the model 
* `training_dataset_path` - this will register the training dataset with datarobot.  

After you run the script for the first time, you entry will now look like 

```
- deployment_id: 6852c645f551d3fa719af3b0
  name: quantile-0.05 external test deployment
  prediction_environment_id: 68517d02a038d3c56d0bbb40
  registered_model_id: 6852c644d46b4f470a4a1193
  registered_model_version_id: 6852d527a6bbf64d1e4a11be
  target_name: charges
  target_type: Regression
  training_data_path: data/training_data_v2.csv
  training_dataset_id: 68404e7745fc9db4b112d002
  url: https://app.datarobot.com/console-nextgen/deployments/6852c645f551d3fa719af3b0
  ```

The first time you run this script, it will create `deployment_id`, `registered_model_id`, and `registered_model_version_id`.  When running this later (once you might have refreshed to model), We wil maintain the same `deployment_id` and `registered_model_id`, and we will create a new `registered_model_version_id`.  

## `create_custom_inference_deployment.py`

Currently only supported with structured regression and structured binary classification problems.  

This will do the following 

* test the custom code locally before taking it into datarobot via drum (only works on mac or linux machines - not windows)
* create a custom model in the model workshop in datarobot
* add the code artifacts
* build the custom model environment
* Register the model in datarobot
* create the registered model package
* deploys the model as a managed endpoint (could be used to deploy to AKS)

This will result in a prediction endpoint that can be utilized for realtime or batch prediction.  

This currently only supports binary classification and regression problems.

The imput is a yaml containing a list of details.  each entry in the list should provide the following 
* `artifact_folder` is location of the model and all assets that comply with DataRObot Custome Models
* `environment_id` datarobot environment id paired with the custom model.  
* `name` - name of your model
* `prediction_environment_id` - existing prediction environment id to be used when deploying model 
* `target_name` - name of the target from your model
* `target_type` - target type of model, currently this script only supports Regression and Binary
* `positive_class_label` - if applicable
* `negative_class_label` - if applicable
* `training_dataset_id` - id of training dataset registered with datarobot
* `training_dataset_path` - path to the training dataset.  

Concerning `training_dataset_id` and `training_dataset_path`, you must provide one of the following setups in the conf
* `training_dataset_id` and `training_dataset_path` - this registers a new version of the dataset with datarobot based on the file located at `training_dataset_path`.  
* `training_dataset_id` - this will use a previosuly registered dataset as the training dataset for the model 
* `training_dataset_path` - this will register the training dataset with datarobot.  

Example 

```
- artifact_folder: /Users/timothy.whittaker/Desktop/git/dr-umbrella-models/models/quantile-0.05
  drum_test_data_path: data/training_data_v2.csv
  environment_id: 5e8c889607389fe0f466c72d
  name: quantile-0.05 test3
  prediction_environment_id: 66a929580c3e174abc7542cb
  target_name: charges
  target_type: Regression
  training_dataset_id: 68404e7745fc9db4b112d002
```
and after running the script your first time, the result will look like 

```
- artifact_folder: /Users/timothy.whittaker/Desktop/git/dr-umbrella-models/models/quantile-0.05
  custom_model_id: 685956f8a95c75425e9c207e
  custom_model_version_id: 685956fa9313a59c2a14d4f7
  deployment_id: 685959a213581a2d0a45a5cb
  drum_test_data_path: data/training_data_v2.csv
  drum_test_passed: true
  environment_build_status: success
  environment_id: 5e8c889607389fe0f466c72d
  includes_requirements: true
  name: quantile-0.05 test3
  prediction_environment_id: 66a929580c3e174abc7542cb
  realtime_endpoint: https://app.datarobot.com/api/v2/deployments/685959a213581a2d0a45a5cb/predictions
  registered_model_id: 6859578edf61c2e306b9cf50
  registered_model_version_id: 6859578fdf61c2e306b9cf52
  target_name: charges
  target_type: Regression
  training_dataset_id: 68404e7745fc9db4b112d002
```
