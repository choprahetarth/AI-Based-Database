# AI-Based-Database
A PoC for a database which uses machine-learning in order to create new rows in a database. 

### True North
query = """SELECT AVG(LENGTH(topic))
        FROM ai_closest_topic
        JOIN ai_sentiment_analysis ON ai_closest_topic.id = ai_sentiment_analysis.id
        WHERE ai_sentiment_analysis.sentiment = 'false
        ';"""

### things to be specified by database admin
1 - Database Schema (use functions to specify) # in a YAML file
2 - Input of ML Models (from schema) # REST API
3 - Output of ML Models (in the schema)
4 - ML Model execution Logic
xmin is not present, and hence should be populated via ML Model

### step 1 - make a strcutured table (using YAML)
### step 2 - provide a button to populate the tabel using MongoDB
### step 3 - provide the mappings of the ML Model.

