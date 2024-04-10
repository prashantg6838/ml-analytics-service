#!/bin/bash

# Read config.ini and extract file list
echo "Reading config.ini and extracting file list..."

FILE_LIST=$(cat config.ini | grep 'faust_applications_list' | cut -d '=' -f 2 | sed "s/\[//;s/\]//;s/'//g")
echo "The file list is: $FILE_LIST"

# Start each service listed in the config.ini file
echo "Starting services..."
# Split the FILE_LIST into an array
IFS=',' read -r -a services <<< "$FILE_LIST"
for service in "${services[@]}"; do
    # Extract service path and arguments
    service_path=$(echo "$service" | awk -F ' ' '{print $1}')
    service_args=$(echo "$service" | awk -F ' ' '{print $2}')
    echo "Starting service: $service_path with arguments: $service_args"
    /opt/sparkjobs/faust_as_service/faust.sh "$service_path" "$service_args" &
done

echo "Waiting for background processes to finish..."
wait -n

# Log exit status
exit_status=$?
echo "Exit status: $exit_status"

# Exit with the exit status of the last command executed
exit $exit_status
