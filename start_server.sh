
export MLOPS_SPOOLER_TYPE="FILESYSTEM"
export MLOPS_FILESYSTEM_DIRECTORY="/tmp/ta"
export MLOPS_DEPLOYMENT_ID="dummy_id_1234"
export MLOPS_MODEL_ID="dummy_id_4321"
export DEPLOYMENT_ID="dummy_id_1234" ## dummy id for umbrella model
export MODEL_ID="dummy_id_4321"      ## dummy model package id for umbrella model

## run montiroing age
export JAVA_HOME="/usr/lib/jvm/java-11-openjdk/"
export MLOPS_SERVICE_URL=https://app.datarobot.com
export MLOPS_API_TOKEN=$DATAROBOT_API_TOKEN

echo "starting drum server"
drum server --code-dir ./custom-model --target-type unstructured --address 0.0.0.0:12345 --logging-level info --verbose