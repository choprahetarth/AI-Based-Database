from flask import Flask, jsonify, request
from flask_cors import CORS
import requests


app = Flask(__name__)


# using a free API for sentiment analysis 
url = "https://api.apilayer.com/sentiment/analysis"



@app.route('/analyze_sentiment', methods=['GET'])
def analyze_sentiment():
    tweet = request.args.get('text')
    if tweet:
        payload = tweet
        headers= {
        "apikey": "T7z2uSqTq9YBRR6dgpQ5crLgTSq93XJl"
        }

        response = requests.request("POST", url, headers=headers, data = payload)
        status_code = response.status_code
        result = response.text
        # sentiment = "positive" if result['label'] == 'positive' else "negative"
        return jsonify(sentiment=result), 200
    else:
        return jsonify(error="No text provided"), 400
    
@app.route('/', methods=['GET'])
def home_page():
    return "Hello World"
    
if __name__ =='__main__':
    app.run()
