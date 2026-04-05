import paho.mqtt.client as mqtt
from pymongo import MongoClient
from datetime import datetime
import json
import threading
import time
import logging
from flask import Flask, jsonify, request
from flask_cors import CORS
from bson.objectid import ObjectId

# ---------- LOGGING ----------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ---------- CONFIGURATION ----------
MQTT_BROKER = "w150161e.ala.eu-central-1.emqxsl.com"
MQTT_PORT = 8883
MQTT_USER = "HenHouse"
MQTT_PASS = "Mouna1817"

TOPIC_SENSORS = "mouna/farm/sensors"
TOPIC_COMMANDS = "mouna/farm/app_commands"
TOPIC_ACTUATORS = "mouna/farm/arduino_actuators"

MONGO_URI = "mongodb+srv://HenHouse_db_user:Mouna1817@cluster0.cgzyo5y.mongodb.net/"
MONGO_DB = "HenHouse_db"
MONGO_COLLECTION = "sensors_log"

# ---------- GLOBAL STATE ----------
mqtt_client = None
mongo_client = None
db = None
collection = None
mqtt_connected = False

# ---------- FLASK APP ----------
app = Flask(__name__)
CORS(app)

@app.route('/api/history', methods=['GET'])
def get_history():
    """Fetch last 20 sensor records from MongoDB"""
    try:
        if collection is None:
            return jsonify({'error': 'Database not connected'}), 500
        
        # Get last 20 records sorted by created_at descending
        records = list(collection.find().sort('created_at', -1).limit(20))
        
        # Remove MongoDB ObjectId and convert to JSON-serializable format
        for record in records:
            del record['_id']
            if 'created_at' in record and hasattr(record['created_at'], 'isoformat'):
                record['created_at'] = record['created_at'].isoformat()
        
        # Reverse to get ascending order (oldest first)
        records.reverse()
        logger.info(f"[API] Returned {len(records)} history records")
        return jsonify(records), 200
    except Exception as e:
        logger.error(f"[API] Error fetching history: {e}")
        return jsonify({'error': str(e)}), 500

# ---------- MONGODB CONNECTION ----------
def connect_mongodb():
    """Establish MongoDB connection with retry logic"""
    global mongo_client, db, collection
    retry_count = 0
    max_retries = 5
    
    while retry_count < max_retries:
        try:
            mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            # Test connection
            mongo_client.admin.command('ping')
            db = mongo_client[MONGO_DB]
            collection = db[MONGO_COLLECTION]
            logger.info("[MongoDB] Connected successfully")
            return True
        except Exception as e:
            retry_count += 1
            logger.warning(f"[MongoDB] Connection failed (attempt {retry_count}/{max_retries}): {e}")
            if retry_count < max_retries:
                time.sleep(5)
    
    logger.error("[MongoDB] Failed to connect after all retries")
    return False

def disconnect_mongodb():
    """Safely disconnect MongoDB"""
    global mongo_client
    if mongo_client is not None:
        try:
            mongo_client.close()
            logger.info("[MongoDB] Disconnected")
        except:
            pass
        mongo_client = None

# ---------- MQTT CALLBACKS ----------
def on_mqtt_connect(client, userdata, flags, rc):
    """Called when MQTT connects"""
    global mqtt_connected
    if rc == 0:
        mqtt_connected = True
        logger.info("[MQTT] Connected successfully (code 0)")
        # Subscribe to both sensor and command topics
        client.subscribe(TOPIC_SENSORS)
        client.subscribe(TOPIC_COMMANDS)
        logger.info(f"[MQTT] Subscribed to {TOPIC_SENSORS} and {TOPIC_COMMANDS}")
    else:
        mqtt_connected = False
        logger.error(f"[MQTT] Connection failed with code {rc}")

def on_mqtt_disconnect(client, userdata, rc):
    """Called when MQTT disconnects"""
    global mqtt_connected
    mqtt_connected = False
    if rc != 0:
        logger.warning(f"[MQTT] Unexpected disconnection (code {rc}). Will attempt to reconnect...")
    else:
        logger.info("[MQTT] Disconnected")

def on_mqtt_message(client, userdata, msg):
    """Called when MQTT message arrives"""
    try:
        topic = msg.topic
        payload = msg.payload.decode('utf-8')
        logger.info(f"[MQTT] Message received on {topic}: {payload[:100]}")
        
        if topic == TOPIC_SENSORS:
            # Sensor data from Arduino
            handle_sensor_data(json.loads(payload))
        
        elif topic == TOPIC_COMMANDS:
            # Command from web dashboard
            handle_dashboard_command(client, json.loads(payload))
    
    except json.JSONDecodeError as e:
        logger.error(f"[MQTT] Failed to parse JSON: {e}")
    except Exception as e:
        logger.error(f"[MQTT] Error processing message: {e}")

def handle_sensor_data(data):
    """Process incoming sensor data and save to MongoDB"""
    try:
        # Add timestamp
        data['created_at'] = datetime.utcnow()
        
        # Save to MongoDB
        if collection is not None:
            result = collection.insert_one(data)
            logger.info(f"[DB] Sensor data saved (ID: {result.inserted_id})")
        else:
            logger.error("[DB] Collection not available, skipping save")
    
    except Exception as e:
        logger.error(f"[DB] Error saving sensor data: {e}")

def handle_dashboard_command(client, command):
    """Forward dashboard command to Arduino actuators"""
    try:
        logger.info(f"[CMD] Command from dashboard: {command}")
        
        # Forward to Arduino actuators topic
        payload = json.dumps(command)
        client.publish(TOPIC_ACTUATORS, payload, qos=1)
        logger.info(f"[MQTT] Command forwarded to {TOPIC_ACTUATORS}")
    
    except Exception as e:
        logger.error(f"[CMD] Error forwarding command: {e}")

# ---------- MQTT CONNECTION ----------
def connect_mqtt():
    """Establish MQTT connection with retry logic"""
    global mqtt_client
    retry_count = 0
    max_retries = 5
    
    while retry_count < max_retries:
        try:
            mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id=f"henhouse_bridge_{int(time.time())}")
            mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)
            mqtt_client.on_connect = on_mqtt_connect
            mqtt_client.on_disconnect = on_mqtt_disconnect
            mqtt_client.on_message = on_mqtt_message
            
            # Setup TLS
            mqtt_client.tls_set()
            
            # Connect
            mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            logger.info(f"[MQTT] Connection initiated to {MQTT_BROKER}:{MQTT_PORT}")
            
            # Start connection loop in background
            mqtt_client.loop_start()
            
            # Wait for connection to establish
            for i in range(10):
                if mqtt_connected:
                    return True
                time.sleep(1)
            
            logger.warning("[MQTT] Connection established but callback not triggered within timeout")
            return True
        
        except Exception as e:
            retry_count += 1
            logger.warning(f"[MQTT] Connection failed (attempt {retry_count}/{max_retries}): {e}")
            if retry_count < max_retries:
                time.sleep(5)
    
    logger.error("[MQTT] Failed to connect after all retries")
    return False

def disconnect_mqtt():
    """Safely disconnect MQTT"""
    global mqtt_client
    if mqtt_client is not None:
        try:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()
            logger.info("[MQTT] Disconnected")
        except:
            pass
        mqtt_client = None

# ---------- MQTT MONITOR THREAD ----------
def mqtt_monitor_thread():
    """Monitor and reconnect MQTT if needed"""
    global mqtt_client, mqtt_connected
    
    while True:
        try:
            time.sleep(10)
            
            if not mqtt_connected and mqtt_client is not None:
                logger.warning("[MQTT] Connection lost, attempting to reconnect...")
                try:
                    mqtt_client.reconnect()
                except:
                    # If reconnect fails, create new connection
                    disconnect_mqtt()
                    connect_mqtt()
        
        except Exception as e:
            logger.error(f"[Monitor] Error in MQTT monitor: {e}")

# ---------- FLASK THREAD ----------
def run_flask():
    """Run Flask in a separate thread"""
    import os
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"[Flask] Starting Flask server on http://0.0.0.0:{port}")
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

# ---------- MAIN ----------
def main():
    """Main initialization and coordination"""
    logger.info("=" * 60)
    logger.info("HenHouse IoT Bridge Starting")
    logger.info("=" * 60)
    
    # Connect to MongoDB
    if not connect_mongodb():
        logger.error("Failed to connect to MongoDB, continuing anyway...")
    
    # Connect to MQTT
    if not connect_mqtt():
        logger.error("Failed to connect to MQTT, continuing anyway...")
    
    # Start MQTT monitor thread
    monitor_thread = threading.Thread(target=mqtt_monitor_thread, daemon=True)
    monitor_thread.start()
    logger.info("[Thread] MQTT monitor thread started")
    
    # Start Flask thread
    flask_thread = threading.Thread(target=run_flask, daemon=False)
    flask_thread.start()
    logger.info("[Thread] Flask API thread started")
    
    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        disconnect_mqtt()
        disconnect_mongodb()
        logger.info("Bridge stopped")

if __name__ == '__main__':
    main()
