# Umbrella

## Files

* `runner.ipynb` - simple notebook that mocks the get date, preprocess data, score data, write predictions type of pipeline.  This type of flow is what would be scheduled in DataRobot.  

* `./src` - contains dummy files for `get_data`, `preprocess` data, `postprocess` data, and `write_predictions`.  The `score.py` file will consume data, chunk it if necessary, and send data to the umbrella model.  The umbrella model will take care of the routing to the individual regression models.  

* `./models` - contains ALL model artifacts for the umbrella model as well as the quantile regressions.  Based on the way I build the quantile regressions, using sklearn pipelines, transformers, and estimators, all that was required is the serialized model artifact (pkl), but the umbrella model routes data, so it is a little more involved.  At the moment the umbrella model returns a list of dictionaries.  Each dictionary has key, value pairs, where the keys are: tag, data.  Tag corresponds to the quantile, and data corresponds to the returned predictions.  

## Approach 1

One Unstructured Monitored Custom DataRobot Model will serve as an entry to point to 19 sub models - each is a quantile regressions. Each regression has been deployed as its own DataRobot Deployment which will provide visibility into predictions overtime as well as provide the  ability to monitor target drift once we report actuals back to each individual deployment.  

Feature drift will be tracked for each model by way of the umbrella model.  

See the main umbrella model in `./models/master-model`

### Recommended if ...

* You might need to only retrain a single model.
* you require prediction tracking for all submodels
* you require feature drift monitoring

### Benefit to approach 1

Allows replacement of any model, without disrupting other models.  

## Approach 2

Similar to the first, but instead of the unstructured model being used to call 19 other deployments, all of the model artifacts are included in this model and scoring happens entirely within the unstructured models (no calls to other datarobot deployments for predictions).  All submodels have a spot in the deployment console, but only for the purposes of monitoring.  This approach will still provide feature drift monitoring, target drift monitoring, and capture predictions overtime.  

### Recommended if ...

* you will retrain all sub models at a given go.  
* you require prediction tracking for all submodels
* you require feature drift monitoring

### Benefit to approach 2

This will probably be quite a bit faster than approach 1 as network calls are not being made to the 19 sub models deployed as datarobot endpoints.  

Less deployments sitting in AKS.  Previous approach would require 20 AKS Deployments, 1 for the master and 19 for the sub models.  This would require 1.  







