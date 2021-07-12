# voucher_selection_api
An API which returns a Voucher value for a customer based on order history and segments.

![Screenshot 2021-07-10 at 9 43 55 PM](https://user-images.githubusercontent.com/9393761/125169504-26776600-e1c8-11eb-8bf1-626a07631d78.png)

The implementation at hand has 3 essential tasks
* EDA: Exploratory Data Analysis and Cleansing is required to understand the historic data, types, abnormalities and hence the data can be cleansed. 
       EDA is performed with the help of Jupyter Notebooks. The Notebook can be found [here](https://github.com/roshanzameer/voucher_selection_api/blob/main/notebooks/EDA.ipynb).
       
* Pipeline: A pipeline was needed to orchestrate the data extraction from S3, its cleansing, loading to DB. Hence, Apache Airflow is used. 
* Rest API: A REST API exposed a POST /voucher endpoint which returns the voucher amount. 

### Service Packaging

All the pipelines/frameworks/tools that were needed to build the system have been dockerized. The pipeline has 3 integral components
* Postgres Database: The cleansed historic data and the segments generated are written to the Database.
* Airflow Webserver: A Sequential Executor instance of Airflow image was built and committed to the Dockerhub public repository.
* Flask API: The Flask REST API is containerised also.


### Deployment

#### Pipeline

* The service, no matter how scalable they can be, are subtely linked with each other. The deployment is very straight forward owing to Docker Compose.
* The *docker compose up* command spawn the 3 containers. 
              
`docker compose up`
              
* The containers are loaded in the order of the dependecies. 
* On loading, the Apache Airflow container build its meta Database. (SQLite currently)
* Once the services are loaded, the pipeline can be visited from here: [localhost:8080](http://localhost:8080)
* Once the Airflow UI is accessible, the Dag Voucher_Select is to be triggered by switching it on.

#### REST API
* The Docker-Compose script also spins up a Container which runs the Flask based REST API
* The API exposes a POST /voucher endpoint on the 8000 port, which returns the Voucher amount.
* A sample Curl request is shown below.

       curl --location --request POST 'localhost:8000/voucher' \
            --header 'Content-Type: application/json' \
            --data-raw '{
               "customer_id": 123,
               "country_code": "Peru",
               "last_order_ts": "2018-05-03 00:00:00",
               "first_order_ts": "2017-05-03 00:00:00",
               "total_orders": 3,
               "segment_name": "frequent_segment"
               }'
* Sample Response
              `{
                  "voucher_amount": 2640
              }`      
             


           



