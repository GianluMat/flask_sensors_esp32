from flask import Flask, request, jsonify
from pymongo import MongoClient
from datetime import datetime
import os

app = Flask(__name__)

client = None

def get_db():
    global client
    if client is None:
        # Crea l'istanza di MongoClient solo se non esiste ancora
        mongo_uri = "mongodb+srv://GianITS:ProjectITS33@clusterits.do6lt.mongodb.net/?retryWrites=true&w=majority&appName=ClusterITS"
        client = MongoClient(mongo_uri)
    return client["sensors_db"]

@app.route('/api/sensordata', methods=['POST'])
def receive_sensor_data():
    try:
        data = request.json
        data['timestamp'] = datetime.now(datetime.UTC)
        db = get_db()
        collection = db["sensors_data"]
        collection.insert_one(data)
        
        return jsonify({"message": "Data saved successfully"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/sensordata', methods=['GET'])
def get_sensor_data():
    try:
        db = get_db()
        collection = db["sensors_data"]
        data = list(collection.find({}, {"_id": 0}))
        return jsonify(data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5050, debug=True)