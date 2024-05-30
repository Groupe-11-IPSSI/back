from app import app
from flask import request, jsonify
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from joblib import load
from dotenv import load_dotenv
import os
import pandas as pd
import math

load_dotenv()
 
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
 
db_pool = pool.ThreadedConnectionPool(1, 30, user=DB_USER, password=DB_PASS, host=DB_HOST, database=DB_NAME)

best_rf_model = load('app/models/best_model_gold_top_25.pkl')
best_rf_model_silver = load('app/models/best_model_silver_top_25.pkl')
best_rf_model_bronze = load('app/models/best_model_bronze_top_25.pkl')

def get_db_connection():
    conn = db_pool.getconn()
    if conn:
        return conn
    else:
        raise Exception("Connection pool exhausted")

def release_db_connection(conn):
    if conn:
        db_pool.putconn(conn)

def with_db_connection(f):
    def decorated_function(*args, **kwargs):
        conn = get_db_connection()
        try:
            return f(conn, *args, **kwargs)
        finally:
            release_db_connection(conn)
    decorated_function.__name__ = f.__name__
    return decorated_function

country_name_mapping = {
    'Federal Republic of Germany': 'Germany',
    'German Democratic Republic (Germany)': 'Germany',
    'Soviet Union': 'Russia',
    'Czechoslovakia': 'Czech Republic',
    'Yugoslavia': 'Serbia',
    'West Indies Federation': 'Jamaica',  
    'Unified Team': 'Russia',  
    'Republic of Korea': 'South Korea',
    "Democratic People's Republic of Korea": 'North Korea',
    'ROC': 'Russia',
    "CÃ´te d'Ivoire": "Côte d'Ivoire"
}
 
@app.route('/', methods=['GET'])
def home():
    return "Hackathon Groupe 11 - API !"

@app.route('/countries', methods=['GET'])
@with_db_connection
def get_countries(conn):
    query = '''
        SELECT DISTINCT country_name
        FROM result_summer
        ORDER BY country_name;
    '''
    
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(query)
    countries = [row['country_name'] for row in cur.fetchall()]

    for i, country in enumerate(countries):
        if country in country_name_mapping:
            countries[i] = country_name_mapping[country]

    cur.close()
    
    return jsonify(countries)

@app.route('/hosts', methods=['GET'])
@with_db_connection
def get_hosts(conn):
    query = '''
        SELECT * 
        FROM hosts
        ORDER BY game_year DESC;
    '''

    conn = db_pool.getconn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(query)
    hosts = cur.fetchall()
    cur.close()

    return jsonify(hosts)

@app.route('/years', methods=['GET'])
@with_db_connection
def get_years(conn):
    query = '''
        SELECT DISTINCT game_year
        FROM result_summer
        ORDER BY game_year DESC;
    '''

    conn = db_pool.getconn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(query)
    years = [row['game_year'] for row in cur.fetchall()]
    cur.close()

    return jsonify(years)

@app.route('/predict', methods=['GET'])
@with_db_connection
def predict(conn):
    data = pd.read_csv('app/data/X_2024_top_25_for_prediction.csv')
    X_2024_top_25 = data.drop(['total_medals', 'gold_medals', 'silver_medals', 'bronze_medals', 'country_name'], axis=1)

    prediction_gold = best_rf_model.predict(X_2024_top_25)
    prediction_silver = best_rf_model_silver.predict(X_2024_top_25)
    prediction_bronze = best_rf_model_bronze.predict(X_2024_top_25)

    data['predicted_gold_medals'] = prediction_gold
    data['predicted_silver_medals'] = prediction_silver
    data['predicted_bronze_medals'] = prediction_bronze

    data_selected = data[["country_name", "sport_played", "predicted_gold_medals", "predicted_silver_medals", "predicted_bronze_medals"]]
    data_selected["predicted_gold_medals"] = data_selected["predicted_gold_medals"].apply(math.ceil)
    data_selected["predicted_silver_medals"] = data_selected["predicted_silver_medals"].apply(math.ceil)
    data_selected["predicted_bronze_medals"] = data_selected["predicted_bronze_medals"].apply(math.ceil)
    data_sorted = data_selected.sort_values(by="predicted_gold_medals", ascending=False)

    predictions = data_sorted.to_dict(orient='records')

    return jsonify(predictions)

@app.route('/athletes', methods=['GET'])
@with_db_connection
def get_athletes(conn):
    country = request.args.get('country_name')

    if country:
        query = '''
            SELECT * 
            FROM athletes
            WHERE country_name = %s
            ORDER BY total_medals DESC
            LIMIT 10;
        '''
        params = (country,)
    else:
        query = '''
            SELECT * 
            FROM athletes
            ORDER BY total_medals DESC
            LIMIT 10;
        '''
        params = None

    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(query, params)
    athletes = cur.fetchall()
    cur.close()

    return jsonify(athletes)
 
@app.route('/medals', methods=['GET'])
@with_db_connection
def get_medals(conn):
    year = request.args.get('year')
    country = request.args.get('country_name')

    if year:
        query = '''
            SELECT
                country_name,
                COUNT(medal_type) AS total_medals,
                COUNT(CASE WHEN medal_type = 'GOLD' THEN 1 END) AS gold_count,
                COUNT(CASE WHEN medal_type = 'SILVER' THEN 1 END) AS silver_count,
                COUNT(CASE WHEN medal_type = 'BRONZE' THEN 1 END) AS bronze_count
            FROM result_summer
            WHERE game_year = %s AND medal_type IS NOT NULL
            GROUP BY country_name
            ORDER BY total_medals DESC;
        '''
        params = (year,)

    if country:
        query = '''
            SELECT 
                country_name, 
                game_year,
                COUNT(medal_type) AS total_medals,
                COUNT(CASE WHEN medal_type = 'GOLD' THEN 1 END) AS gold_count,
                COUNT(CASE WHEN medal_type = 'SILVER' THEN 1 END) AS silver_count,
                COUNT(CASE WHEN medal_type = 'BRONZE' THEN 1 END) AS bronze_count
            FROM result_summer
            WHERE country_name = %s AND medal_type IS NOT NULL
            GROUP BY country_name, game_year
            ORDER BY game_year;
        '''
        params = (country,)

    else:
        query = '''
            SELECT
                country_name,
                COUNT(medal_type) AS total_medals,
                COUNT(CASE WHEN medal_type = 'GOLD' THEN 1 END) AS gold_count,
                COUNT(CASE WHEN medal_type = 'SILVER' THEN 1 END) AS silver_count,
                COUNT(CASE WHEN medal_type = 'BRONZE' THEN 1 END) AS bronze_count
            FROM result_summer
            WHERE medal_type IS NOT NULL
            GROUP BY country_name
            ORDER BY total_medals DESC;
        '''
        params = None
    
    conn = db_pool.getconn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(query, params)
    medals = cur.fetchall()
    cur.close()

    return jsonify(medals)
 
if __name__ == '__main__':
    app.run(threaded=True)
