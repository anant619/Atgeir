input_args=$#

if [ $input_args -eq  1 ]
then
	echo metadata ingestion for ${1}
	datahub ingest -c ${1}
	error_code=$?
	if [ $error_code != 0 ]
	then
	       	echo "metadata ingestion has failed.."
	fi
		echo "metadata ingestion Comleted.."
else
    printf "%b" "Argument count incorrect. Stopping processing...\n"
fi
