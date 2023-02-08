A deployment in Prefect is a server-side concept that encapsulates a flow, allowing it to be scheduled and triggered via the API. 

A flow can have multiple deployments and you can think of it as the container of metadata needed for the flow to be scheduled. This might be what type of infrastructure the flow will run on, or where the flow code is stored, maybe it’s scheduled or has certain parameters. 

There are two ways to create a deployment. One is using the CLI command and the other is with python. Jeff will show how to set up the deployment with Python in the next video so for now we are going to create one using the CLI. 

Inside your terminal we can type  `prefect deployment build ./parameterized_flow.py:etl_parent_flow -n "Parameterized ETL"`

Now you can see it created a yaml file with all our details. This is the metadata. We can adjust the parameters here or in the UI after we apply but let’s just do it here:
- edit yaml with ` parameters: { "color": "yellow", "months" :[1, 2, 3], "year": 2021}`

Now we need to apply the deployment: `prefect deployment apply etl_parent_flow-deployment.yaml`

Here we can see the deployment in the UI, trigger a flow run, and you’ll notice this goes to late. That is because we are now using the API to trigger and schedule flow runs with this deployment instead of manually and we need to have an agent living in our execution environment (local) 

Below is the snapshot of the Prefect flow deployment

![image](https://user-images.githubusercontent.com/988040/217627673-9596891d-d39d-4fde-b830-6c6937e5c083.png)

Link of Slack notification when the job ran from the Prefect UI
https://temp-notify.slack.com/archives/C04NPH686M9/p1675881176525779
