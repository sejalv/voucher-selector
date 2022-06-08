# Voucher Selector

## Objective

A pipeline & API for voucher selection built with Airflow, Postgres, Flask, and Docker.

## Problem

The task is to create a Voucher Selection API for the country: Peru

There are 3 steps that should be done:

0. Conduct data analysis to explore and prepare the data.
1. Create a data pipeline to generate customer segments, including data cleaning, optimization.
2. Create a REST API that will expose the most used voucher value for a particular customer
segment.

### Dataset


## Solution
  
For **Data exploration** (step 0), you could setup a Jupyter Notebook along with a test interface (such as Jupyter Lab) to explore the given dataset. Due to limited time, I've chosen to skip that, but you can look at the transformations (airflow tasks/utils) to get more understanding.

### To setup and run the Data pipeline:
 
 1. First-time setup: Airflow local auth
    ```
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    ```
 
 2. Build (for requirements):
    ```
    docker compose build
    ```
 
 3. Initialize airflow config:
    ```
    docker compose up airflow-init
    ```
 
 4. Kick up containers for postgres, airflow scheduler + webserver, flask-api
    ```
    docker compose up
    ```
 
 5. Run pipeline

    From command line:
    ```
    docker-compose exec webserver airflow trigger_dag voucher_selector
    ```

    Or trigger from Airflow web UI: `http://127.0.0.1:8080/`

### Finally, to call the Flask API

(`voucher_segments`)

```
curl -X GET -H "Content-type: application/json" -d '{"customer_id": 123, "country_code": "Peru", "last_order_ts": "2018-05-03 00:00:00", "first_order_ts": "2017-05-03 00:00:00", "total_orders": 15, "segment_name": "recency_segment"}' "http://localhost:5000/voucher_amount"
```
Output:  `{"voucher_amount":[2640.0]}`
   
   
 ### Closing and cleanup
 
 Close docker session, when everything is done
   ```
   docker-compose down
   ```


 ### Components

  #### Database
   Tech: `postgres:13.0`
   
   * Schema: `voucher_customer`
   * Tables: `customer_segments` (input), `voucher_segments` (backend-api), `voucher_selector` (other storage)
   * Connection: `{'postgres_default': 'postgresql+psycopg2://airflow:airflow@postgres/airflow'}`
  
  #### API
  
  The Flask API (http://localhost:5000) which queries from `voucher_customer.voucher_segments`   

  #### Orchestration
  
   Tech: `apache/airflow:2.3.0`, with Python 3.7
   
   **Notes**:
   * Used [official Docker setup](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html) provided by apache-airflow.
   * Customized it to set `LocalExecutor` as Core Executor, `postgres_default` connection for airflow, and used a build-image to install requirements

   **Pipeline Flow**

   1. Generates `voucher_customer.customer_segments` table by loading data from `customer_segments.sql`
   
   2. Fetches voucher-data from the input parquet-gzip file. (Used HTTP source URL, since S3 source not working)

   3. Processes voucher-data and maps to customer-segments (for region Peru)

   4. Updates the `voucher_customer.voucher_segments` table (daily), with the count of vouchers used for a particular segment_type
      
      ```
      $ docker-compose exec postgres psql -U airflow
      psql
      Type "help" for help.

      airflow=# select * from voucher_customer.voucher_segments;
       index | min_range | max_range |   segment_name   | voucher_amount | count 
      -------+-----------+-----------+------------------+----------------+-------
           0 |       180 |  99999999 | recency_segment  |              0 | 13950
           1 |       180 |  99999999 | recency_segment  |           2640 | 49102
           2 |       180 |  99999999 | recency_segment  |           3520 | 22037
           3 |       180 |  99999999 | recency_segment  |           4400 | 21458
           0 |         0 |         4 | frequent_segment |              0 |  4543
           1 |         0 |         4 | frequent_segment |           2640 | 16496
           2 |         0 |         4 | frequent_segment |           3520 |  7758
           3 |         0 |         4 | frequent_segment |           4400 |  7402
           4 |         5 |        13 | frequent_segment |              0 |   253
           5 |         5 |        13 | frequent_segment |           2640 |  4112
           6 |         5 |        13 | frequent_segment |           3520 |  1374
           7 |         5 |        13 | frequent_segment |           4400 |  1272
           8 |        14 |        37 | frequent_segment |              0 |  1501
           9 |        14 |        37 | frequent_segment |           2640 | 11813
          10 |        14 |        37 | frequent_segment |           3520 |  4391
          11 |        14 |        37 | frequent_segment |           4400 |  4225
          12 |        38 |  99999999 | frequent_segment |              0 |  7653
          13 |        38 |  99999999 | frequent_segment |           2640 | 16681
          14 |        38 |  99999999 | frequent_segment |           3520 |  8514
          15 |        38 |  99999999 | frequent_segment |           4400 |  8559
      (20 rows)

      ```  
  

### Test

(TBD: Config error)
```
$ docker-compose exec webserver python -m unittest -v
```


## Future Enhancements
* Parallelization of tasks (workers) possible, use of CeleryExecutor
* ORM and security in Flask API
* Setup Jupyter notebook interface
