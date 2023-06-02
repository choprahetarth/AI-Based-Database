from flask import Flask, jsonify, request
from flask_cors import CORS


app = Flask(__name__)
# CORS(app)  # Enable CORS

# # Initialize sentiment analysis pipeline
# sentiment_analysis = pipeline("sentiment-analysis")

@app.route('/analyze_sentiment', methods=['GET'])
def analyze_sentiment():
    tweet = request.args.get('text')
    if tweet:
        # result = sentiment_analysis(tweet)[0]
        # sentiment = "Positive" if result['label'] == 'POSITIVE' else "Negative"
        return jsonify(sentiment="j"), 200
    else:
        return jsonify(error="No text provided"), 400
    
@app.route('/', methods=['GET'])
def home_page():
    return "Hello World"
    
if __name__ =='__main__':
    app.run()
