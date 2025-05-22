# Umbrella

## Files

* `create_deployments_approach_2.ipynb` - This notebook will
      * create 19 quantile regressions and deploy them to datarobot as an external model.
      * test out the main umbrella / master model -> spins up a local inference server so you can test making predictions exactly as you would with datarobot
      * deploys the umbrella model to dr and test making predictions with it

* `./custom-model` - contains ALL model artifacts for the umbrella model as well as the quantile regressions.  Based on the way I build the quantile regressions, using sklearn pipelines, transformers, and estimators, all that was required is the serialized model artifact (pkl), but the umbrella model routes data, so it is a little more involved.  At the moment the umbrella model returns a list of dictionaries.  Each dictionary has key, value pairs, where the keys are: tag, data.  Tag corresponds to the quantile, and data corresponds to the returned predictions.  

## Approach 

All of the model artifacts are included in the unstructured umbrella model and scoring happens entirely within the unstructured models (no calls to other datarobot deployments for predictions).  All submodels have a spot in the deployment console, but only for the purposes of monitoring.  This approach will still provide feature drift monitoring, target drift monitoring, and capture predictions overtime.  

### Recommended if ...

* you will retrain all sub models at a given go.  
* you require prediction tracking for all submodels
* you require feature drift monitoring

### Benefit to approach

This will probably be quite a bit faster than approach 1 as network calls are not being made to the 19 sub models deployed as datarobot endpoints.  

1 deployment sitting in AKS

### Considerations 

If model is truely batch, we might benefit from a different monitoring approach.  







