from app import app
from flask import request, jsonify
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
 
DB_HOST = 'postgresql-hackaton-jo-11.alwaysdata.net'
DB_NAME = 'hackaton-jo-11_db_propre'
DB_USER = 'hackaton-jo-11_api'
DB_PASS = 'Nwk8!G!NgXfGdk3'
 
db_pool = psycopg2.pool.ThreadedConnectionPool(1, 20, user=DB_USER, password=DB_PASS, host=DB_HOST, database=DB_NAME)
 
@app.route('/', methods=['GET'])
def home():
    return "Hackathon Groupe 11 - API !"

@app.route('/countries', methods=['GET'])
def get_countries():
    query = '''
        SELECT DISTINCT country_name
        FROM result_summer
        ORDER BY country_name;
    '''
    
    conn = db_pool.getconn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(query)
    countries = [row['country_name'] for row in cur.fetchall()]
    cur.close()
    conn.close()
    
    return jsonify(countries)

@app.route('/hosts', methods=['GET'])
def get_hosts():
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
    conn.close()

    return jsonify(hosts)

@app.route('/years', methods=['GET'])
def get_years():
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
    conn.close()

    return jsonify(years)

@app.route('/athletes', methods=['GET'])
def get_athletes():
    query = '''
        SELECT * 
        FROM athletes
        ORDER BY total_medals DESC;
    '''

    conn = db_pool.getconn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(query)
    athletes = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify(athletes)
 
@app.route('/medals', methods=['GET'])
def get_medals():
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
    conn.close()

    return jsonify(medals)
 
if __name__ == '__main__':
    app.run(threaded=True)
