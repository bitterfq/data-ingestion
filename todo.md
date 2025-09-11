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

1. Document
   1. learnings
   2. archeitecture
2. Showcase work
3. Setup automation instead of remembering pyspark command
4. Figure out optimization
   1. Airbyte optimizaiton instead of rewriting each time