from app import app
from flask import request, jsonify
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from joblib import load
 
DB_HOST = 'postgresql-hackaton-jo-11.alwaysdata.net'
DB_NAME = 'hackaton-jo-11_db_propre'
DB_USER = 'hackaton-jo-11_api'
DB_PASS = 'Nwk8!G!NgXfGdk3'
 
db_pool = psycopg2.pool.ThreadedConnectionPool(1, 20, user=DB_USER, password=DB_PASS, host=DB_HOST, database=DB_NAME)

best_rf_model = load('app/models/best_rf_model_gold_2024.joblib')
# best_rf_model_silver = load('./models/best_rf_model_silver_2024.joblib')
# best_rf_model_bronze = load('./models/best_rf_model_bronze_2024.joblib')

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
    country = request.args.get('country_name')

    if not country:
        return jsonify({ 'error' : 'Missing required query parameter: country_name' }), 400

    query = '''
        SELECT *
        FROM result_summer
        WHERE country_name = %s
    '''
    params = (country,)

    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(query, params)
    result = cur.fetchall()

    cur.close()

    return jsonify(result)

    # prediction = best_rf_model.predict(result)

    # print(prediction)

@app.route('/athletes', methods=['GET'])
@with_db_connection
def get_athletes(conn):
    query = '''
        SELECT * 
        FROM athletes
        ORDER BY total_medals DESC;
    '''

    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(query)
    athletes = cur.fetchall()
    cur.close()

    return jsonify(athletes)
 
@app.route('/medals', methods=['GET'])
@with_db_connection
def get_medals(conn):
    year = request.args.get('year')

    if year:
        query = '''
            SELECT
                country_name,
                COUNT(medal_type) AS total_medals,
                COUNT(CASE WHEN medal_type = 'GOLD' THEN 1 END) AS gold_count,
                COUNT(CASE WHEN medal_type = 'SILVER' THEN 1 END) AS silver_count,
                COUNT(CASE WHEN medal_type = 'BRONZE' THEN 1 END) AS bronze_count
            FROM result_summer
            WHERE game_year = %s
            GROUP BY country_name
            ORDER BY total_medals DESC;
        '''
        params = (year,)
    else:
        query = '''
            SELECT
                country_name,
                COUNT(medal_type) AS total_medals,
                COUNT(CASE WHEN medal_type = 'GOLD' THEN 1 END) AS gold_count,
                COUNT(CASE WHEN medal_type = 'SILVER' THEN 1 END) AS silver_count,
                COUNT(CASE WHEN medal_type = 'BRONZE' THEN 1 END) AS bronze_count
            FROM result_summer
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
