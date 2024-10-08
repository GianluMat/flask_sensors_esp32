from flask import Flask, request, jsonify
from flask_cors import CORS
from pymongo import MongoClient
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
import threading
import json

app = Flask(__name__)

CORS(app, 
    #  resources={r"/*": {"origins": ["http://example.com", "http://anotherdomain.com"]}},
     methods=["GET", "POST", "PUT", "DELETE"],
     allow_headers=["Content-Type", "Authorization"],
    #  supports_credentials=True,
    #  max_age=3600
)

client = None

def get_db():
    global client
    if client is None:
        # Crea l'istanza di MongoClient solo se non esiste ancora
        mongo_uri = "mongodb+srv://GianITS:ProjectITS33@clusterits.do6lt.mongodb.net/?retryWrites=true&w=majority&appName=ClusterITS"
        client = MongoClient(mongo_uri)
    return client["sensors_db"]

# Funzione di callback per i messaggi MQTT
def on_message(client, userdata, message):
    try:
        payload = message.payload.decode("utf-8")
        print(f"Messaggio ricevuto su {message.topic}: {payload}")
        data = json.loads(payload)
        data['timestamp'] = datetime.now(timezone.utc)
        db = get_db()
        collection = db["sensors_data"]
        collection.insert_one(data)
    except Exception as e:
        print("An error occurred:", e)

def on_connect(client, userdata, flags, rc, properties):
    print(f"Connesso con codice di ritorno {rc}")
    # Iscrivi solo una volta
    client.subscribe("gian33home/homesensors/#")

# Configura il client MQTT
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

# Imposta le funzioni di callback
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

def mqtt_loop():
    mqtt_client.connect("broker.hivemq.com", 1883, 60)
    mqtt_client.loop_forever()

# Avvia il thread MQTT solo se non è già avviato
if not hasattr(threading.current_thread(), 'mqtt_thread'):
    mqtt_thread = threading.Thread(target=mqtt_loop, daemon=True)
    mqtt_thread.start()

@app.route('/api/sensors/data', methods=['POST'])
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

@app.route('/api/sensors/data', methods=['GET'])
def get_sensor_data():
    try:
        db = get_db()
        collection = db["sensors_data"]
        sensor = request.args.get('sensor')
        location = request.args.get('location')
        query_filter = {}
        if sensor:
            query_filter['sensor'] = sensor
        if location:
            query_filter['location'] = location
        data = list(collection.find(query_filter, {"_id": 0}))

        return jsonify(data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5050, debug=False)