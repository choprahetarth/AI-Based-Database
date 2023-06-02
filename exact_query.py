import yaml
import psycopg2

def read_exact_query():
    query = """SELECT AVG(LENGTH(topic))
               FROM ai_closest_topic
               JOIN ai_sentiment_analysis ON ai_closest_topic.id = ai_sentiment_analysis.id
               WHERE ai_sentiment_analysis.sentiment = 'false';"""