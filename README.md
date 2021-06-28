# (Adapt README to this project)

# dl_sparkify

This project is intended to analyze data for a hypothetical start-up called Sparkify. This music streaming start-up wants to analyze their song- and log-related data in a more efficient and risk-free way, by both using AWS Redshift.

The goal of this project is to build data warehousing capabilities for Sparify. More specifically, the song- and log-data (JSON) is stored in AWS S3 buckets, staged into AWS Redshift and subsquently the data is put into a star-schema within a PostgreSQL database within redshift. 

## Staging

![alt text](https://github.com/raul-bermejo/cloud_sparkify/blob/main/images/staging_tables.png)

The raw data produced by the Sparkify app is staged as raw data into Redshift. In other words, the schema from the raw data is preserver, as seen in the figure above.

## Database schema

The implemented database schema can be seen in the ER diagram below.

![alt text](https://github.com/raul-bermejo/cloud_sparkify/blob/main/images/sparkify_erd_transparent.png)

The Entity Relationshiip Diagram (ERD) above is a Star Schema where the facts (or metrics) are represented by the songplays relation. The reason for this is to have the analysis of log and song data at the heart of the business. From the songplays relation one can observe the dimension of the sparkify business: users, artists, songs and time. Each of these relations represents a core business aspect of sparkify.

## ETL pipeline

The raw data stored in S3 buckets, is copied into the staging tables above in Redshift. Next, the data is routed from the staging tables into the Star-Schema for analytical query optimization. Note the ETL pipeline is implemented with a combination of Python for AWS and PostreSQL connection combined with the use of SQL queries.

## Dependencies

The following libraries need to be installed for the code to work: numpy, pandas, boto3, and psycopg2.

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install any of these libraries if needed.

```bash
pip install numpy
pip install pandas
pip install psycopg2
pip install boto3
```

## Authors

The author of this repo is me, Raul Bermejo, as part of the Data Engineer program at Udacity.

## Usage

To load the data into Redshift using the code, the order of execution is:

(1) Create Redshift cluster using AWS (either progarmatically or through the consolole)
(2) Run create_tables.py to create the SQL tables  (both staging and star-schema)
(3) Run etl.py to load the data into Redshift
(4) Make sure you delete the Redshift cluster once the data is loaded

## Contributing
Pull requests are welcome. For other issues and/or changes, feel free open an issue.
