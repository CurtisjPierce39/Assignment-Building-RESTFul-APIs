from flask import Flask, jsonify, request
from flask_marshmallow import Marshmallow
from marshmallow import fields, ValidationError
import mysql.connector
from mysql.connector import Error

app = Flask(__name__)
ma = Marshmallow(app)

class MemberSchema(ma.Schema):
    member_id = fields.Int(required=True)
    name = fields.String(required=True)
    age = fields.Int(required=True)

    class Meta:
        fields = ("member_id", "name", "age")

member_schema = MemberSchema()
members_schema = MemberSchema(many=True)

class SessionSchema(ma.Schema):
    session_id = fields.Int(required=True)
    member_id = fields.Int(required=True)
    session_date = fields.Date(required=True)
    duration_minutes = fields.Int(required=True)
    calories_burned = fields.Int(required=True)

    class Meta:
        fields = ("session_id", "member_id", "session_date", "duration_minutes", "calories_burned")
        
session_schema = SessionSchema()
sessions_schema = SessionSchema(many=True)

def get_db_connection(): #Question 1 Task 1
    db_name = 'gym_database_management'
    user = 'root'
    password = 'Cjp007Cjp!'
    host = 'localhost'

    try:
        conn = mysql.connector.connect(
            database = db_name,
            user = user,
            password = password,
            host = host
        )
        print("Connected to MySql Database Successfully!")
        return conn

    except Error as e:
        print(f"Error: {e}")
        return None

@app.route('/')
def home():
    return "Welcome Home"

@app.route("/members", methods=["GET"]) #Question 1 Task 2

#GET Route for members
def get_members():
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor(dictionary=True)

        query = "SELECT * FROM Members"

        cursor.execute(query)

        members = cursor.fetchall()

        return members_schema.jsonify(members)

    except Error as e:
        print(f"Error: {e}")
        return jsonify({"error": "Internal Server Error"}), 500

    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

#ADD Member route
@app.route("/members", methods=["POST"])
def add_member():
    try:
        member_data = member_schema.load(request.json)
    except ValidationError as e:
        print(f"Error: {e}")
        return jsonify(e.messages), 400

    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()

        new_member = (member_data['member_id'], member_data['name'], member_data['age'])

        query = "INSERT INTO Members (member_id, name, age) VALUES (%s, %s, %s)"

        cursor.execute(query, new_member)
        conn.commit()

        return jsonify({"message": "New Member Added Successfully"}), 201
    except Error as e:
        print(f"Error: {e}")
        return jsonify({"error": "Internal Server Error"}), 500
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

#UPDATE Member route
@app.route("/members/<int:id>", methods=["PUT"])
def update_member(id):
    try:
        member_data = member_schema.load(request.json)
    except ValidationError as e:
        print(f"Error: {e}")
        return jsonify(e.messages), 400

    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()

        updated_member = (member_data['member_id'], member_data['name'], member_data['age'], id)

        query = 'UPDATE Members SET member_id = %s, name = %s, age = %s WHERE member_id = %s'

        cursor.execute(query, updated_member)
        conn.commit()

        return jsonify({"message": "Member Updated Successfully"}), 201

    except Error as e:
        print(f"Error: {e}")
        return jsonify({"error": "Internal Server Error"}), 500

    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

#DELETE Member route
@app.route("/members/<int:id>", methods=["DELETE"])
def delete_members(id):
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()

        member_to_remove = (id,)

        cursor.execute("SELECT * FROM Members WHERE member_id = %s", member_to_remove)

        customer = cursor.fetchone()

        if not customer:
            return jsonify({"error": "Member not found"}), 201
        
        query = "DELETE FROM Members WHERE member_id = %s"
        cursor.execute(query, member_to_remove)
        conn.commit()

        return jsonify({"message": "Member Removed Successfully"}), 200

    except Error as e:
        print(f"Error: {e}")
        return jsonify({"error": "Internal Server Error"}), 500

    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

#POST route for Workoutsessions with validation
@app.route("/workoutsessions", methods=["POST"])
def add_session():
    try:
        session_data = session_schema.load(request.json)
    except ValidationError as err:
        return jsonify(err.messages), 400

    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Database connection failed"}), 500

    try:
        cursor = conn.cursor()
        query = "INSERT INTO Workoutsessions (session_id, member_id, session_date, duration_minutes, calories_burned) VALUES (%s, %s, %s, %s, %s)"
        cursor.execute(query, (session_data['session_id'], session_data['member_id'], session_data['session_date'], session_data['duration_minutes'], session_data['calories_burned']))
        conn.commit()
        return jsonify({"message": "New Session Added successfully"}), 201

    except Error as e:
        return jsonify({"error": str(e)}), 500

    finally:
        cursor.close()
        conn.close()

#GET route for all sessions
@app.route('/workoutsessions', methods=['GET'])
def get_orders():
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Database connection failed})"}), 500

    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM Workoutsessions")
    orders = cursor.fetchall()
    cursor.close()
    conn.close()
    return sessions_schema.jsonify(orders)

#PUT route for orders with validation
@app.route('/workoutsessions/<int:session_id>', methods=['PUT'])
def update_session(session_id):
    try:
        session_data = session_schema.load(request.json)
    except ValidationError as err:
        return jsonify(err.messages), 400

    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Database connection failed"}), 500

    try:
        cursor = conn.cursor()
        query = "UPDATE Workoutsessions SET session_id = %s, member_id = %s, session_date = %s, duration_minutes = %s, calories_burned = %s WHERE session_id = %s"
        cursor.execute(query, (session_data['session_id'], session_data['member_id'], session_data['session_date'], session_data['duration_minutes'], session_data['calories_burned'], session_id))
        conn.commit()
        return jsonify({"message": "Session Updated Successfully"}), 200

    except Error as e:
        return jsonify({"error": str(e)}), 500

    finally:
        cursor.close()
        conn.close()
#DELETE route for sessions

@app.route('/workoutsessions/<int:session_id>', methods=['DELETE'])
def delete_session(session_id):
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Database connection failed"}), 500

    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM Workoutsessions WHERE session_id = %s", (session_id,))
        conn.commit()
        return jsonify({"message": "Session Deleted Successfully"}), 200

    except Error as e:
        return jsonify({"error": str(e)})

    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    app.run(debug=True)
