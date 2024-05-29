from app import app

from flask import request, jsonify

import psycopg2

from psycopg2 import pool

from psycopg2.extras import RealDictCursor
 
DB_HOST = 'postgresql-hackaton-jo-11.alwaysdata.net'

DB_NAME = 'hackaton-jo-11_db_propre'

DB_USER = 'hackaton-jo-11_api'

DB_PASS = 'Nwk8!G!NgXfGdk3'
 
# Initialize connection pool

db_pool = psycopg2.pool.ThreadedConnectionPool(1, 20, user=DB_USER, password=DB_PASS, host=DB_HOST, database=DB_NAME)
 
@app.route('/', methods=['GET'])

def home():

    return "Hackathon Groupe 11 - API !"
 
@app.route('/medals', methods=['GET'])

def get_medals():

    params = {

        'medal_type': request.args.get('medal_type'),

        'country_name': request.args.get('country_name'),

        'event_gender': request.args.get('event_gender'),

        'participant_type': request.args.get('participant_type'),

        'year': request.args.get('year'),

        'game_season': request.args.get('game_season')

    }
 
    valid_params = { key: value for key, value in params.items() if value }
 
    query = '''

      SELECT medals.*, athletes.*, hosts.game_season

      FROM medals

      JOIN athletes ON medals.athlete_id = athletes.athlete_id

      JOIN hosts ON medals.game_slug = hosts.game_slug

    '''
 
    conditions = []

    values = []
 
    for key, value in valid_params.items():

        if key == 'medal_type' and value not in ['GOLD', 'SILVER', 'BRONZE']:

            return jsonify({ 'error': 'Invalid medal_type parameter' }), 400

        if key == 'event_gender' and value not in ['Men', 'Women', 'Mixed']:

            return jsonify({ 'error': 'Invalid event_gender parameter' }), 400

        if key == 'participant_type' and value not in ['Athlete', 'GameTeam']:

            return jsonify({ 'error': 'Invalid participant_type parameter' }), 400

        if key == 'game_season' and value not in ['Winter', 'Summer']:

            return jsonify({ 'error': 'Invalid game_season parameter' }), 400

        if key == 'year':

            conditions.append("game_slug LIKE %s")

            value = f"%{value}"

        else:

            conditions.append(f"{key} = %s")

        values.append(value)
 
    if conditions:

        query += ' WHERE ' + ' AND '.join(conditions)
 
    conn = db_pool.getconn()

    try:

        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute(query, values)

        medals = cur.fetchall()

        cur.close()

    except Exception as e:

        return jsonify({'error': str(e)}), 500

    finally:

        db_pool.putconn(conn)

    return jsonify(medals)
 
if __name__ == '__main__':

    app.run(threaded=True)
