## Cloud-Based Data Processing (IN2386)
* URL Shortener

## Build server binary
* Can be found in the following directory: build/bin

```
$ make build_server
```

## Build client binary
* Can be found in the following directory: build/bin

```
$ make build_client
```

## Build server docker image

```
$ make release_server
```

## Build client docker image

```
$ make release_client
```

## Tests

```
$ make test
```

## Deploying to Azure
* Set the following env variables first:
```
$ REGISTRY_NAME=
$ ACI_PERS_STORAGE_ACCOUNT_NAME=
$ ACI_PERS_RESOURCE_GROUP=
$ ACI_PERS_LOCATION=

$ STORAGE_KEY=$(az storage account keys list \
               --resource-group $ACI_PERS_RESOURCE_GROUP
               --account-name $ACI_PERS_STORAGE_ACCOUNT_NAME
               --query "[0].value" --output tsv)
```
* Create a volume for each server
```
$ az storage share create --name <server-volname-1> --account-name $ACI_PERS_STORAGE_ACCOUNT_NAME
$ az storage share create --name <server-volname-2> --account-name $ACI_PERS_STORAGE_ACCOUNT_NAME
...
$ az storage share create --name <server-volname-n> --account-name $ACI_PERS_STORAGE_ACCOUNT_NAME
```
* Create a volume for the client
```
$ az storage share create --name <client-volname> --account-name $ACI_PERS_STORAGE_ACCOUNT_NAME 
```
* Tag the server and client containers
```
$ docker tag kv-server "$REGISTRY_NAME.azurecr.io"/kv-server
$ docker tag client "$REGISTRY_NAME.azurecr.io"/client 
```
* Push the containers
```
$ docker push "$REGISTRY_NAME.azurecr.io"/kv-server
$ docker push "$REGISTRY_NAME.azurecr.io"/client
```
* Create the servers (e.g. 3 servers)
```
$ az container create \
	--resource-group cbdp-resourcegroup \
	--name kv-server0 \
	--image "$REGISTRY_NAME.azurecr.io"/kv-server \
	--azure-file-volume-account-name $ACI_PERS_STORAGE_ACCOUNT_NAME \
	--ports 43203 44578 \
	--restart-policy Never \
	--dns-name-label <server-volname-0> \
	--azure-file-volume-account-key $STORAGE_KEY \
	--azure-file-volume-share-name $ACI_PERS_SHARE_NAME \
	--azure-file-volume-mount-path /tmp/ \
	--command-line "/bin/kv-server -a kv-server0.westeurope.azurecontainer.io -p 43203 -ll DEBUG -d /tmp/process_data -s LFU -c 1024 -n kv-server0.westeurope.azurecontainer.io:43203 -n kv-server1.westeurope.azurecontainer.io:44671 -n kv-server2.westeurope.azurecontainer.io:44577"

$ az container create \
	--resource-group cbdp-resourcegroup \
	--name kv-server1 \
	--image "$REGISTRY_NAME.azurecr.io"/kv-server \
	--azure-file-volume-account-name $ACI_PERS_STORAGE_ACCOUNT_NAME \
	--ports 44671 44578 \
	--restart-policy Never \
	--dns-name-label kv-server1 \
	--azure-file-volume-account-key $STORAGE_KEY \
	--azure-file-volume-share-name <server-volname-1> \
	--azure-file-volume-mount-path /tmp/ \
	--command-line "/bin/kv-server -a kv-server1.westeurope.azurecontainer.io -p 44671 -ll DEBUG -d /tmp/process_data -s LFU -c 1024 -n kv-server0.westeurope.azurecontainer.io:43203 -n kv-server1.westeurope.azurecontainer.io:44671 -n kv-server2.westeurope.azurecontainer.io:44577"

$ az container create \
	--resource-group cbdp-resourcegroup \
	--name kv-server2 \
	--image "$REGISTRY_NAME.azurecr.io"/kv-server \
	--azure-file-volume-account-name $ACI_PERS_STORAGE_ACCOUNT_NAME \
	--ports 44577 44578 \
	--restart-policy Never \
	--dns-name-label kv-server2 \
	--azure-file-volume-account-key $STORAGE_KEY \
	--azure-file-volume-share-name <server-volname-2> \
	--azure-file-volume-mount-path /tmp/ \
	--command-line "/bin/kv-server -a kv-server2.westeurope.azurecontainer.io -p 44577 -ll DEBUG -d /tmp/process_data -s LFU -c 1024 -n kv-server0.westeurope.azurecontainer.io:43203 -n kv-server1.westeurope.azurecontainer.io:44671 -n kv-server2.westeurope.azurecontainer.io:44577"
```
* Add input.csv file (manually) to \<client-volname\> and create the client
```
$ az container create \
	--resource-group cbdp-resourcegroup \
	--name client \
	--image "$REGISTRY_NAME.azurecr.io"/client \
	--azure-file-volume-account-name $ACI_PERS_STORAGE_ACCOUNT_NAME \
	--restart-policy Never \
	--azure-file-volume-account-key $STORAGE_KEY \
	--azure-file-volume-share-name client \
	--azure-file-volume-mount-path /tmp/ \
	--command-line "/bin/client kv-server0.westeurope.azurecontainer.io:43203 kv-server1.westeurope.azurecontainer.io:44671 kv-server2.westeurope.azurecontainer.io:44577 -push /tmp/input.csv"
```
* Wait for the previous command to finish (check the logs)
```
$ az container logs --name <client-name> -g cbdp-resourcegroup
```
* Request each key from the database and store the results in output.csv
```
$ az container create \
    --resource-group cbdp-resourcegroup \
    --name client \
    --image "$REGISTRY_NAME.azurecr.io"/client \
    --azure-file-volume-account-name $ACI_PERS_STORAGE_ACCOUNT_NAME \
    --restart-policy Never \
    --azure-file-volume-account-key $STORAGE_KEY \
    --azure-file-volume-share-name client \
    --azure-file-volume-mount-path /tmp/ \
    --command-line "/bin/client kv-server0.westeurope.azurecontainer.io:43203 kv-server1.westeurope.azurecontainer.io:44671 kv-server2.westeurope.azurecontainer.io:44577 -pull /tmp/input.csv /tmp/output.csv"
```
* Compare input.csv and output.csv (should be the same)
