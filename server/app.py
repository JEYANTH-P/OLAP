from flask import Flask, jsonify, request
import psycopg2
import logging

app = Flask(__name__)

logging.basicConfig(level=logging.DEBUG)

def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname='student_dropout_analysis',
            user='postgres',
            password='Jeyanth@2004',
            host='localhost'
        )
        return conn
    except psycopg2.DatabaseError as e:
        logging.error(f"Database connection error: {e}")
        return None

@app.route('/cube_sales')
def cube_sales():
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Database connection error"}), 500
    try:
        cur = conn.cursor()
        cur.execute('''
            SELECT
                r.region_name,
                t.year,
                t.month,
                t.day,
                s.standard_name,
                cube_ll_coord(cube_data, 4)::NUMERIC AS no_of_students
            FROM student_dropout_fact
            JOIN region_dim r ON cube_ll_coord(cube_data, 1) = r.region_id
            JOIN time_dim t ON cube_ll_coord(cube_data, 2) = t.time_id
            JOIN standard_dim s ON cube_ll_coord(cube_data, 3) = s.standard_id
        ''')
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify(rows)
    except Exception as e:
        logging.error(f"Error fetching cube sales data: {e}")
        return jsonify({"error": "Error fetching cube sales data"}), 500

@app.route('/cube_rollup')
def cube_rollup():
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Database connection error"}), 500
    try:
        cur = conn.cursor()
        cur.execute('''
            SELECT
                COALESCE(r.region_name, 'All Regions') AS region_name,
                COALESCE(t.year, 'All Years') AS year,
                COALESCE(s.standard_name, 'All Standards') AS standard_name,
                SUM(cube_ll_coord(cube_data, 4)::NUMERIC) AS total_students
            FROM student_dropout_fact
            JOIN region_dim r ON cube_ll_coord(cube_data, 1) = r.region_id
            JOIN time_dim t ON cube_ll_coord(cube_data, 2) = t.time_id
            JOIN standard_dim s ON cube_ll_coord(cube_data, 3) = s.standard_id
            GROUP BY CUBE(r.region_name, t.year, s.standard_name)
        ''')
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify(rows)
    except Exception as e:
        logging.error(f"Error fetching cube rollup data: {e}")
        return jsonify({"error": "Error fetching cube rollup data"}), 500

@app.route('/cube_drilldown_region')
def cube_drilldown_region():
    region_id = request.args.get('region_id')
    if not region_id:
        return jsonify({"error": "Missing region_id parameter"}), 400
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Database connection error"}), 500
    try:
        cur = conn.cursor()
        cur.execute('''
            WITH RECURSIVE region_hierarchy AS (
                SELECT region_id, region_name, parent_region_id
                FROM region_dim
                WHERE region_id = %s
                UNION ALL
                SELECT r.region_id, r.region_name, r.parent_region_id
                FROM region_dim r
                INNER JOIN region_hierarchy rh ON r.parent_region_id = rh.region_id
            )
            SELECT
                rh.region_name,
                t.year,
                t.month,
                t.day,
                s.standard_name,
                cube_ll_coord(cube_data, 4)::NUMERIC AS no_of_students
            FROM student_dropout_fact
            JOIN region_hierarchy rh ON cube_ll_coord(cube_data, 1) = rh.region_id
            JOIN time_dim t ON cube_ll_coord(cube_data, 2) = t.time_id
            JOIN standard_dim s ON cube_ll_coord(cube_data, 3) = s.standard_id
        ''', (region_id,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify(rows)
    except Exception as e:
        logging.error(f"Error fetching cube drilldown data: {e}")
        return jsonify({"error": "Error fetching cube drilldown data"}), 500

@app.route('/cube_rollup_region')
def cube_rollup_region():
    year = request.args.get('year')
    if not year:
        return jsonify({"error": "Missing year parameter"}), 400
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Database connection error"}), 500
    try:
        cur = conn.cursor()
        cur.execute('''
            SELECT
                COALESCE(r.region_name, 'All Regions') AS region_name,
                t.month,
                s.standard_name,
                SUM(cube_ll_coord(cube_data, 4)::NUMERIC) AS total_students
            FROM student_dropout_fact
            JOIN region_dim r ON cube_ll_coord(cube_data, 1) = r.region_id
            JOIN time_dim t ON cube_ll_coord(cube_data, 2) = t.time_id
            JOIN standard_dim s ON cube_ll_coord(cube_data, 3) = s.standard_id
            WHERE t.year = %s
            GROUP BY ROLLUP(r.region_name, t.month, s.standard_name)
        ''', (year,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify(rows)
    except Exception as e:
        logging.error(f"Error fetching cube rollup data: {e}")
        return jsonify({"error": "Error fetching cube rollup data"}), 500

@app.route('/cube_slice')
def cube_slice():
    region_name = request.args.get('region_name')
    year = request.args.get('year')
    standard_name = request.args.get('standard_name')
    
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Database connection error"}), 500
    try:
        cur = conn.cursor()
        query = '''
            SELECT
                r.region_name,
                t.year,
                t.month,
                t.day,
                s.standard_name,
                cube_ll_coord(cube_data, 4)::NUMERIC AS no_of_students
            FROM student_dropout_fact
            JOIN region_dim r ON cube_ll_coord(cube_data, 1) = r.region_id
            JOIN time_dim t ON cube_ll_coord(cube_data, 2) = t.time_id
            JOIN standard_dim s ON cube_ll_coord(cube_data, 3) = s.standard_id
            WHERE 1=1
        '''
        params = []
        if region_name:
            query += ' AND r.region_name = %s'
            params.append(region_name)
        if year:
            query += ' AND t.year = %s'
            params.append(year)
        if standard_name:
            query += ' AND s.standard_name = %s'
            params.append(standard_name)
        
        cur.execute(query, tuple(params))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify(rows)
    except Exception as e:
        logging.error(f"Error fetching cube slice data: {e}")
        return jsonify({"error": "Error fetching cube slice data"}), 500

if __name__ == '__main__':
    app.run(debug=True)