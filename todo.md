
# Sep 03

1. Implement testing for parts generator [DONE]
2. document all choices for the data_generator [DONE]
3. code variables into ENV variable for option
4. data generation files should go in data_generator/data [DONE]
5. add timer to generator, so we can guage performance [N/A]

# Sep 04

1. setup PUB/SUB FOR TOPIC 1 -> AIRBYTE [DONE]
2. setup airbyte -> raw listeners [DONE]
3. add another service for kafka to docker compose [DONE]
4. make sure data is being added to the data_generator/data file [DONE]

# Sep 05 

1. clean up infra, make sure it makes sense
2. document airbyte bridge w/ kafka connector
3. setup iceberg and silver storage
4. think about raw -> silver being in iceberg or just silve in iceberg
5. evaluate if kafka is actually a smart choice w/ Airbyte
   1. Do a write up, really think about this one
6. comment on code bride_service.py [DONE]
7. organize code better -> figure out organization