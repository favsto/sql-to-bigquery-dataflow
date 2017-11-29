# SQL to BigQuery Dataflow pipeline
This project is a basic Java pipeline developed with Google Dataflow 2.1.0 SDK. The purpose is migrating a MS SQL Server catalog to a BigQuery dataset.

[Google Cloud Dataflow](https://cloud.google.com/dataflow/) provides a simple, powerful programming model for building both batch and streaming parallel data processing pipelines. This pipeline is designed for a batch migration.

## Getting started

### Requirements
You need a MS SQL Server with a source catalog and a [Google Cloud Console](https://cloud.google.com/console) project.

General requirements:
- Java 7 SDK
- Apache Maven 
- your favourite code editor

MS SQL Server requirements:
- You must let this **database reachable from where the pipeline will execute**. You can extablish a VPN tunnel between its network and Google VPC Network; at least (*but not suggested*) you could expose this database publicly.
- You need to have **credentials** for read the catalog and information_schema

Google project requirements:
- A Google user with sufficient IAM roles to use Dataflow and BigQuery.
- The project must be associated to a valid billing account.
- Compute Engine, Dataflow, Storage and BigQuery API enabled.

## Run code 
In order to inject your custom parameters into the execution you need to specify a set of information, such as:
- project: Google Cloud Console project onto deploy this pipeline and where reside BigQuery output dataset
- temp location: a Google Cloud Storage path for Dataflow to stage any temporary files; the bucket must exists before the run
- staging location: a Google Cloud Storage path for Dataflow to stage binary/compiled files; the bucket must exists before the run
- MS SQL address reachable from the pipeline when it will run on Dataflow, such as VM external IP address
- MS SQL address reachable from your launcher machine
- MS SQL catalog name to migrate
- MS SQL username and password
- BigQuery output dataset name: this dataset must be created before the run

### Run locally
Clone this repository in a your local folder. Use your shell, with maven configured, and enter this folder. Execute the pipeline locally with this command (replace your custom parameters):
```bash
mvn compile exec:java \
    -Dexec.mainClass=it.injenia.dataflow.MsSqlMigrator \
    -Dexec.args="--project=<google cloud project mnemonic id> \
    --tempLocation=gs://<bucket>/<path> \
    --stagingLocation=gs://<bucket>/<path> \
    --sqlAddressFromLauncher=<SQL instance external address>  \
    --sqlPort=<*optional SQL port, default 1433> \
    --sqlUsername=<username>  \
    --sqlPassword=<password> \
    --sqlCatalog=<catalog name> \
    --bigQueryDataset=<bq dataset name> \
    --runner=DirectRunner \
    --sqlAddressFromPipeline=[SQL instance external address]"
```
As you can see, the main difference is the address of SQL Server. The value of *sqlAddressFromPipeline* is often the same to the value of *sqlAddressFromLauncher*. Sometimes you could specify an internal VPN network address when Dataflow uses a SQL instance installed on a GCE VM. This is an optimization because VPC internal traffic is more efficient then using the external address.


### Run on Dataflow
If you would run this code on Dataflow use this command:
```bash
mvn compile exec:java \
    -Dexec.mainClass=it.injenia.dataflow.MsSqlMigrator \
    -Dexec.args="--project=<google cloud project mnemonic id> \
    --tempLocation=gs://<bucket>/<path> \
    --stagingLocation=gs://<bucket>/<path> \
    --sqlAddressFromLauncher=<SQL instance external address>  \
    --sqlPort=<*optional SQL port, default 1433> \
    --sqlUsername=<username>  \
    --sqlPassword=<password> \
    --sqlCatalog=<catalog name> \
    --bigQueryDataset=<bq dataset name> \
    --runner=DataflowRunner \
    --sqlAddressFromPipeline=[SQL instance address visible from Dataflow, maybe an internal address]"
```

For more information about pipeline options visit the [documentation](https://cloud.google.com/dataflow/pipelines/specifying-exec-params).

## License

Apache License Version 2.0

## Contact us
I'm glad if you help us to improve this pipeline. Feel free to interact with us, collaborate and improve this code. Please contact us via e-mail: fausto.fusaro@injenia.it. Any contributor is my friend!
