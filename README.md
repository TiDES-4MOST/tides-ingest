# TiDES Communication Pipeline

The goal of this deliverable is to establish a communication infrastructure to orchestrate the discovery of transients from LSST, compare the objects to the selection functions in D3.3.3 and then feed these objects to the 4MOST Transients API.

![The Flow executing on prefect](FlowD334.gif)

## Requirements

Along with several standard python libraries, the following are required to run this deliverable

```
prefect
prefect_dask
lasair
json
random
yaml
pandas
numpy
sqlalchemy
submit_transients (proprietary 4MOST API)
```
## Deployment

This software has been deployed on the Somerville machine. If you want to deploy this on your own machine, make sure you have Prefect set up following the instructions here: [https://docs.prefect.io/latest/](https://docs.prefect.io/latest/)

We then need to build the deployment like this:

```
$ prefect deployment build -n digby -p lsstuk -q test tidesCom.py:executeCommPipe

-n is just some name for the deployment
-p is the pool
-q is the work-queue
```
See here for more info on deployments: [https://docs.prefect.io/latest/concepts/deployments/](https://docs.prefect.io/latest/concepts/deployments/)

This will create a file called `executeCommPipe-deployment.yaml`

Go into this file and edit the schedule if you want to run this script like a cronjob. See: [https://docs.prefect.io/latest/concepts/schedules/](https://docs.prefect.io/latest/concepts/schedules/)

Then you can apply the deployment:

```
prefect deployment apply executeCommPipe-deployment.yaml
```

I have Prefect configure with my Prefect Cloud account but you can also have the prefect database running locally. I leave it up to you.

