#!/bin/bash

#cd /CMS/CostNomics/costnomics-backend/scripts

input_args=$#

job_type=${1}
client=${2}

export client_id=${client}
export config_dir=./config.properties
export client_email=${4}

echo "Executing" ${job_type} "job for client" ${client_id}

if [ $input_args -eq 3 ]
then

  job_param=${3}

  python3 ${job_param}.py
  error_code=$?

  if [ $error_code != 0 ]
  then
    echo "${job_param}.py  has failed.."
  else
    echo "${job_param}.py Completed.."
  fi
else
    printf "%b" "Argument count incorrect. Stopping processing...\n"
fi
