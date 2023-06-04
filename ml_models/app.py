from flask import Flask, jsonify, request
import requests
import json


app = Flask(__name__)


@app.route('/analyze_sentiment', methods=['GET'])
def analyze_sentiment():
    tweet = request.args.get('text')
    if tweet:
        # using a free API for sentiment analysis
        url = "https://api.apilayer.com/sentiment/analysis"
        payload = tweet
        headers= {
        "apikey": "T7z2uSqTq9YBRR6dgpQ5crLgTSq93XJl"
        }

        response = requests.request("POST", url, headers=headers, data = payload)
        status_code = response.status_code
        result = response.text
        result = json.loads(result)
        # sentiment = "positive" if result['label'] == 'positive' else "negative"
        return jsonify(sentiment=result['sentiment']), status_code
    else:
        return jsonify(error="No text provided"), 400


@app.route('/ner', methods=['GET'])
def provide_ner():
    tweet = request.args.get('text')
    if tweet:
        url = "https://api.apilayer.com/nlp/named_entity?lang=eng"
        payload = tweet
        headers= {
        "apikey": "T7z2uSqTq9YBRR6dgpQ5crLgTSq93XJl"
        }
        response = requests.request("POST", url, headers=headers, data = payload)
        status_code = response.status_code
        result = response.text
        result = json.loads(result)
        return jsonify(ner=result['result']), status_code
    else:
        return jsonify(error="No text provided"), 400


@app.route('/', methods=['GET'])
def home_page():
    return "Hello World"
    
if __name__ =='__main__':
    app.run()
