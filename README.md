# Getting Started with PySpark

This baseline project shows how to interact with PySpark on Spark3 on K8S cluster managed by CML
To begin, open an IPython workbench.

## Cluster permissions

If your CDP Base cluster is Kerberized, you will need to enter your Kerberos
credentials. Click the "Kerberos" tab under your user settings.

Otherwise, you will need to set the HADOOP_USER_NAME environment
variable. Click the "Environment" tab under your project settings.

## Files

Modify the default files to get started with your own project.

* `README.md` -- This project's readme in Markdown format.
* `Spark3_Hbase_Demo1.py` -- PySpark example script to interact with Hbase cluster in CDP Base
* `spark-defaults.conf` -- Stores spark configuration properties, loaded by CML session to run Spark program
