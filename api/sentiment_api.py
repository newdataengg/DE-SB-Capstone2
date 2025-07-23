from flask import Flask, jsonify, request
from flask_cors import CORS
import snowflake.connector
import os
from dotenv import load_dotenv
import json
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

app = Flask(__name__)
CORS(app)

# Snowflake connection configuration
SNOWFLAKE_CONFIG = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA')
}

def get_snowflake_connection():
    """Create and return a Snowflake connection"""
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        return None

@app.route('/api/sentiment/headlines', methods=['GET'])
def get_sentiment_headlines():
    """Get recent headlines with sentiment analysis"""
    try:
        limit = request.args.get('limit', 10, type=int)
        ticker = request.args.get('ticker', None)
        
        conn = get_snowflake_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor()
        
        # Build query based on parameters
        query = """
        SELECT 
            MENTIONED_SYMBOLS_STR,
            TITLE,
            FINBERT_SCORE,
            FINBERT_LABEL,
            PUBLISHED_TIMESTAMP,
            URL
        FROM GOLD_NEWS_SENTIMENT
        WHERE PUBLISHED_TIMESTAMP >= DATEADD(day, -7, CURRENT_TIMESTAMP())
        """
        
        if ticker:
            query += f" AND MENTIONED_SYMBOLS_STR LIKE '%{ticker}%'"
        
        query += f" ORDER BY PUBLISHED_TIMESTAMP DESC LIMIT {limit}"
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        headlines = []
        for row in results:
            headlines.append({
                'symbols': row[0],
                'title': row[1],
                'sentiment_score': float(row[2]) if row[2] else 0,
                'sentiment_label': row[3],
                'published_at': row[4].isoformat() if row[4] else None,
                'url': row[5]
            })
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'headlines': headlines,
            'count': len(headlines),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/sentiment/summary', methods=['GET'])
def get_sentiment_summary():
    """Get aggregated sentiment statistics"""
    try:
        conn = get_snowflake_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor()
        
        query = """
        SELECT 
            FINBERT_LABEL,
            COUNT(*) as count,
            AVG(FINBERT_SCORE) as avg_score
        FROM GOLD_NEWS_SENTIMENT
        WHERE PUBLISHED_TIMESTAMP >= DATEADD(day, -7, CURRENT_TIMESTAMP())
        GROUP BY FINBERT_LABEL
        ORDER BY count DESC
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        summary = {
            'sentiment_distribution': [],
            'total_headlines': 0,
            'average_sentiment': 0
        }
        
        total_count = 0
        total_score = 0
        
        for row in results:
            label, count, avg_score = row
            summary['sentiment_distribution'].append({
                'label': label,
                'count': count,
                'average_score': float(avg_score) if avg_score else 0
            })
            total_count += count
            total_score += float(avg_score) * count if avg_score else 0
        
        summary['total_headlines'] = total_count
        summary['average_sentiment'] = total_score / total_count if total_count > 0 else 0
        summary['timestamp'] = datetime.now().isoformat()
        
        cursor.close()
        conn.close()
        
        return jsonify(summary)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/correlation/stock-sentiment', methods=['GET'])
def get_stock_sentiment_correlation():
    """Get correlation data between stock prices and sentiment"""
    try:
        conn = get_snowflake_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor()
        
        query = """
        SELECT 
            SYMBOL,
            CORRELATION_COEFFICIENT,
            SENTIMENT_AVG,
            PRICE_CHANGE_PCT,
            HEADLINE_COUNT
        FROM GOLD_STOCK_SENTIMENT_CORRELATION
        WHERE CORRELATION_COEFFICIENT IS NOT NULL
        ORDER BY ABS(CORRELATION_COEFFICIENT) DESC
        LIMIT 20
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        correlations = []
        for row in results:
            correlations.append({
                'symbol': row[0],
                'correlation': float(row[1]) if row[1] else 0,
                'sentiment_avg': float(row[2]) if row[2] else 0,
                'price_change_pct': float(row[3]) if row[3] else 0,
                'headline_count': row[4]
            })
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'correlations': correlations,
            'count': len(correlations),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'service': 'Financial Sentiment API'
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True) 