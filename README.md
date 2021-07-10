# dl_sparkify

This project is intended to analyze data for a hypothetical start-up called Sparkify. This music streaming start-up wants to analyze their song- and log-related data in a more efficient and risk-free way, by using the advantages of a **data lake in AWS**

The goal of this project is to build the data lake capabilities for Sparkify. More specifically, the song- and log-data (JSON) is stored in AWS S3 buckets, ETL-ed through sparkify and stored again after the ETL process into S3 again.

## Dataframe schemas

The implemented dataframes schema can be seen in the ER diagram below.

![alt text](https://github.com/raul-bermejo/cloud_sparkify/blob/main/images/sparkify_erd_transparent.png)

The Entity Relationshiip Diagram (ERD) above is a Star Schema where the facts (or metrics) are represented by the songplays relation. The reason for this is to have the analysis of log and song data at the heart of the business. From the songplays relation one can observe the dimension of the sparkify business: users, artists, songs and time. Each of these relations represents a core business aspect of Sparkify.

## ETL pipeline

It is assumed that the raw data is stored in an S3 bucket (I named it 'dl-sparkify'). The script etl.py creates a pyspark session, accesses the raw data in S3 buckets and loads them into dataframes. Then, the raw data is piped into dataframes with the schema shown above.

## Dependencies

The following libraries need to be installed for the code to work: numpy, pandas, boto3, and pyspark.

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install any of these libraries if needed.

```bash
pip install numpy
pip install pandas
pip install pyspark
pip install boto3
```

## Authors

The author of this repo is me, Raul Bermejo, as part of the Data Engineer program at Udacity.

## Usage

To ETL the data  using the code, the order of execution is:

(1) Make sure the raw data resides in S3 buckets ("s3a://dl-sparkify/input/")
(2) Run etl.py to execute the data pipeline

For a more thorough explnanation of how to run the data pipeline in a parallelised (EMR) environment, plese see this [link](https://knowledge.udacity.com/questions/46619#552992).

## Contributing
Pull requests are welcome. For other issues and/or changes, feel free to open an issue.
