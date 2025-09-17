# Sep 03

1. Implement testing for parts generator [DONE]
2. document all choices for the data_generator [DONE]
3. code variables into ENV variable for option [DONE]
4. data generation files should go in data_generator/data [DONE]
5. add timer to generator, so we can guage performance [N/A]

# Sep 04

1. setup PUB/SUB FOR TOPIC 1 -> AIRBYTE [DONE]
2. setup airbyte -> raw listeners [Check Sep 05] [DONE]
3. add another service for kafka to docker compose [DONE]
4. make sure data is being added to the data_generator/data file [DONE]

# Sep 05 

1. clean up infra, make sure it makes sense [DONE]
2. document airbyte bridge w/ kafka connector [N/A]
3. setup iceberg and silver storage [In-Progress]
4. think about raw -> silver being in iceberg or just silver in iceberg [In-Progress]
5. evaluate if kafka is actually a smart choice w/ Airbyte
   1. Do a write up, really think about this one [DONE]
6. comment on code bride_service.py [DONE]
7. organize code better -> figure out organization [DONE]
8. Change landing zone bucket to be called customer-data for better representation [DONE]
9. Implement Airbyte movement of data from customer-data -> raw [DONE]
   1.  Right now its using pandas, which is not ideal at all.

# Sep 08 

1. look at nifi, kafka connect, camel, ceph and debezium [In-Progress]
2. fix processed s3 bucket pathing [DONE]
3. silver iceberg setup [DONE]

# Sep 09

1. clean up kafka infra [DONE]
2. clean up code base [DONE]
3. push and pull codebase [DONE]
4. setup iceberg + spark [DONE]
5. Rotate AWS creds [IN-PROGRESS]
6. Clean branches [DONE]
7. Ask melvin to update diagram [DONE]

# Sep 10 

1. Set up proper stream name pathing for the raw layer on airbyte [DONE]

# Sep 11

1. Document [DONE]
   1. learnings
   2. archeitecture
2. Showcase work [DONE]
3. Figure out optimization [N/A]
   1. Airbyte optimizaiton instead of rewriting each time [N/A]
4. guage end to end performance[DONE]
5. connect db to airbyte to showcase different usecases[Done]

# Sep 12

1. change printout wordings on spark_silver_pipeline.py file [DONE]
2. Schema mismatch between silver and pdf [DONE]
3. writing pg data to silver after running s3 uploads causes schema mismatch [DONE]

# Sep 15

1. change batch md to be upto date [DONE]
2. Make destination pathing agnostic, figure out how to do that.

3. Setup automation instead of remembering pyspark command [DONE]
4. When writing -> airbyte timestamp is UTC and so its a day ahead of S3 [IMPORTANT][USE UTC 0 INSTEAD][STANDARIZATION TIME FORMAT] [DONE]

5. figure out why a spark-warehouse folder gets created using pyspark [DONE]
6. S3 timestamp difference between parts and suppliers in the folders i.e one might be microseconds ahead/different
   1. s3://cdf-upload/tenant_dddd/2025-09-13/20250913_190448/suppliers.csv
      s3://cdf-upload/tenant_dddd/2025-09-13/20250913_190449/parts.csv

# SEP 16

1. GENERATOR -> SNOWFLAKE -> AIRBYTE CONNECTOR SETUP
2. spark -> glue check that angle, bc melvin needs to keep rerunning crawlers to keep up and it breaks. [DONE]
3. figure out what pg connection in airbyte is doing.
4. document bugs
   - double write from pipeline
   - S3 timestamp difference between parts and suppliers in the folders i.e one might be microseconds ahead/different
      s3://cdf-upload/tenant_dddd/2025-09-13/20250913_190448/suppliers.csv
      s3://cdf-upload/tenant_dddd/2025-09-13/20250913_190449/parts.csv
   - Make destination pathing agnostic
   - schema mismatch when double running csv pipeline
5. document learnings