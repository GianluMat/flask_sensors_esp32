from flask import Flask, request, jsonify
from pymongo import MongoClient
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
import threading
import json

app = Flask(__name__)

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
    payload = message.payload.decode("utf-8")
    print(f"Messaggio ricevuto su {message.topic}: {payload}")
    data = json.loads(payload)
    data['timestamp'] = datetime.now(timezone.utc)
    db = get_db()
    collection = db["sensors_data"]
    collection.insert_one(data)

def on_connect(client, userdata, flags, rc, properties):
    print(f"Connesso con codice di ritorno {rc}")
    # Iscrivi solo una volta
    client.subscribe("gian33home/sensors/data")

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
    app.run(host='0.0.0.0', port=5050, debug=False)