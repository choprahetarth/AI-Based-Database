from flask import Flask, jsonify, request
import requests
import random
import json


app = Flask(__name__)

@app.route('/analyze_sentiment', methods=['GET'])
def analyze_sentiment():
    tweet = request.args.get('text')
    if tweet:
        # using a free API for sentiment analysis
        ##### UNCOMMENT IN PROD ########
        # url = "https://api.apilayer.com/sentiment/analysis"
        # payload = tweet
        # headers= {
        # "apikey": "T7z2uSqTq9YBRR6dgpQ5crLgTSq93XJl"
        # }

        # response = requests.request("POST", url, headers=headers, data = payload)
        # status_code = response.status_code
        # result = response.text
        # result = json.loads(result)
        # sentiment = "positive" if result['label'] == 'positive' else "negative"
        ################
        result = {'sentiment':random.randint(0,4)}
        status_code = 200
        return  sentiment=result['sentiment']), status_code
    else:
        return jsonify(error="No text provided"), 400


@app.route('/ner', methods=['GET'])
def provide_ner():
    tweet = request.args.get('text')
    if tweet:
        ##### UNCOMMENT IN PROD ########
        # url = "https://api.apilayer.com/nlp/named_entity?lang=eng"
        # payload = tweet
        # headers= {
        # "apikey": "T7z2uSqTq9YBRR6dgpQ5crLgTSq93XJl"
        # }
        # response = requests.request("POST", url, headers=headers, data = payload)
        # status_code = response.status_code
        # result = response.text
        # result = json.loads(result)
        result = {'result':str(random.randint(5,100000))}
        status_code = 200
        return jsonify(ner=result['result']), status_code
    else:
        return jsonify(error="No text provided"), 400


@app.route('/', methods=['GET'])
def home_page():
    return "Hello World"
    
if __name__ =='__main__':
    app.run()
