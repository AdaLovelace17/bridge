"""
HenHouse Bridge — runs on Render.com
Subscribes to MQTT, saves sensor data to MongoDB, enriches and stores alerts.
"""
import os
import json
import logging
import threading
import time
from datetime import datetime, timedelta

import paho.mqtt.client as mqtt
from pymongo import MongoClient, DESCENDING
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('bridge')

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
MONGO_URI = os.getenv('MONGO_URI', 'mongodb+srv://HenHouse_db_user:Mouna1817@cluster0.cgzyo5y.mongodb.net/')
DB_NAME   = 'HenHouse_db'

MQTT_BROKER = os.getenv('MQTT_HOST', 'w150161e.ala.eu-central-1.emqxsl.com')
MQTT_PORT   = int(os.getenv('MQTT_PORT', '8883'))
MQTT_USER   = os.getenv('MQTT_USER', 'HenHouse')
MQTT_PASS   = os.getenv('MQTT_PASS', 'Mouna1817')

TOPIC_SENSORS  = 'mouna/farm/sensors'
TOPIC_ALERTS   = 'henhouse/alerts'
TOPIC_COMMANDS = 'mouna/farm/app_commands'

TWILIO_SID   = os.getenv('TWILIO_SID', '')
TWILIO_TOKEN = os.getenv('TWILIO_TOKEN', '')
TWILIO_FROM  = os.getenv('TWILIO_FROM', '')

VAPID_PRIVATE = os.getenv('VAPID_PRIVATE', '')
VAPID_PUBLIC  = os.getenv('VAPID_PUBLIC', '')
VAPID_EMAIL   = os.getenv('VAPID_EMAIL', 'admin@henhouse.com')

FLASK_URL = os.getenv('FLASK_URL', '')

# ─────────────────────────────────────────────
# Alert map: type → (severity, Arabic/English title, message)
# ─────────────────────────────────────────────
ALERT_MAP = {
    'fire':       ('critical', '🔥 حريق / Fire Detected',          'Flame sensor triggered — immediate action required'),
    'gas':        ('critical', '☁️ غاز خطير / Dangerous Gas',      'Gas level critical — ventilate immediately'),
    'high_temp':  ('critical', '🌡 حرارة خطيرة / Critical Temp',   'Temperature above maximum threshold'),
    'low_temp':   ('warning',  '🌡 حرارة منخفضة / Low Temp',       'Temperature below minimum threshold'),
    'high_hum':   ('warning',  '💧 رطوبة عالية / High Humidity',   'Humidity above safe level'),
    'low_tank':   ('warning',  '💧 خزان منخفض / Low Tank',         'Main water tank below minimum'),
    'low_trough': ('warning',  '💧 مشرب منخفض / Low Trough',       'Water trough below minimum'),
    'low_food':   ('warning',  '🌾 غذاء منخفض / Low Food',         'Food level below minimum'),
    'pump_block': ('warning',  '⚠️ انسداد / Pump Blockage',        'Pump running with no flow detected'),
}

# ─────────────────────────────────────────────
# Cooldown tracker (in-memory)
# ─────────────────────────────────────────────
alert_cooldowns = {}

def can_alert(farm_id, alert_type, minutes=10):
    key = f'{farm_id}_{alert_type}'
    last = alert_cooldowns.get(key)
    now = datetime.utcnow()
    if last and (now - last).total_seconds() < minutes * 60:
        return False
    alert_cooldowns[key] = now
    return True

# ─────────────────────────────────────────────
# MongoDB
# ─────────────────────────────────────────────
mongo = None
db = None
sensors_col = None
notifs_col = None
users_col = None
MONGO_OK = False

def connect_mongo():
    global mongo, db, sensors_col, notifs_col, users_col, MONGO_OK
    try:
        mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=6000)
        mongo.admin.command('ping')
        db = mongo[DB_NAME]
        sensors_col = db['sensors_log']
        notifs_col  = db['notifications']
        users_col   = db['users']
        MONGO_OK = True
        log.info('MongoDB connected')
    except Exception as e:
        log.error(f'MongoDB connection failed: {e}')
        MONGO_OK = False

connect_mongo()

# ─────────────────────────────────────────────
# Web push
# ─────────────────────────────────────────────
def send_web_push_to_farm(farm_id, title, body):
    if not VAPID_PRIVATE or not MONGO_OK:
        return
    try:
        from pywebpush import webpush, WebPushException
        user = users_col.find_one({'farm_id': farm_id})
        subs = (user or {}).get('push_subscriptions', [])
        for sub in subs:
            try:
                webpush(
                    subscription_info=sub,
                    data=json.dumps({'title': title, 'body': body}),
                    vapid_private_key=VAPID_PRIVATE,
                    vapid_claims={'sub': f'mailto:{VAPID_EMAIL}'},
                )
            except Exception as e:
                log.warning(f'Push failed for sub: {e}')
    except Exception as e:
        log.error(f'send_web_push_to_farm error: {e}')

# ─────────────────────────────────────────────
# Twilio SMS / WhatsApp
# ─────────────────────────────────────────────
def send_sms(to_number, message):
    if not TWILIO_SID or not to_number:
        return
    try:
        from twilio.rest import Client
        client = Client(TWILIO_SID, TWILIO_TOKEN)
        client.messages.create(body=message, from_=TWILIO_FROM, to=to_number)
        log.info(f'SMS sent to {to_number}')
    except Exception as e:
        log.error(f'SMS error: {e}')

def send_whatsapp(to_number, message):
    if not TWILIO_SID or not to_number:
        return
    try:
        from twilio.rest import Client
        client = Client(TWILIO_SID, TWILIO_TOKEN)
        client.messages.create(
            body=message,
            from_=f'whatsapp:{TWILIO_FROM}',
            to=f'whatsapp:{to_number}',
        )
        log.info(f'WhatsApp sent to {to_number}')
    except Exception as e:
        log.error(f'WhatsApp error: {e}')

def notify_user(farm_id, severity, title, body):
    """Send web push and, for critical alerts, SMS/WhatsApp."""
    send_web_push_to_farm(farm_id, title, body)
    if severity == 'critical' and MONGO_OK:
        user = users_col.find_one({'farm_id': farm_id})
        if user:
            phone    = user.get('phone', '')
            whatsapp = user.get('whatsapp', '')
            msg = f'{title}\n{body}'
            if phone:
                send_sms(phone, msg)
            if whatsapp:
                send_whatsapp(whatsapp, msg)

# ─────────────────────────────────────────────
# Handle incoming sensor data
# ─────────────────────────────────────────────
def handle_sensor(payload: dict):
    if not MONGO_OK:
        return
    try:
        payload['created_at'] = datetime.utcnow()
        sensors_col.insert_one(payload)
        farm_id = payload.get('farm_id', 'henhouse_1')
        log.info(f'Sensor saved: farm={farm_id} temp={payload.get("temp")} hum={payload.get("hum")}')
    except Exception as e:
        log.error(f'handle_sensor error: {e}')

# ─────────────────────────────────────────────
# Handle incoming alert from Arduino
# ─────────────────────────────────────────────
def handle_alert(payload: dict):
    if not MONGO_OK:
        return
    try:
        farm_id    = payload.get('farm_id', 'henhouse_1')
        alert_type = payload.get('type', '').lower()
        value      = payload.get('value')
        uptime     = payload.get('uptime')

        if not can_alert(farm_id, alert_type):
            log.info(f'Alert {alert_type} for {farm_id} suppressed (cooldown)')
            return

        info = ALERT_MAP.get(alert_type)
        if not info:
            log.warning(f'Unknown alert type: {alert_type}')
            severity = 'warning'
            title = f'⚠️ Alert: {alert_type}'
            message = f'Alert received from device'
        else:
            severity, title, message = info

        notif = {
            'farm_id':   farm_id,
            'type':      alert_type,
            'severity':  severity,
            'title':     title,
            'message':   message,
            'value':     value,
            'uptime':    uptime,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'read':      False,
            'source':    'arduino',
        }
        notifs_col.insert_one(notif)
        log.info(f'Alert saved: {alert_type} farm={farm_id}')

        notify_user(farm_id, severity, title, message)

    except Exception as e:
        log.error(f'handle_alert error: {e}')

# ─────────────────────────────────────────────
# MQTT
# ─────────────────────────────────────────────
mqtt_client = None

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info('MQTT connected')
        client.subscribe(TOPIC_SENSORS)
        client.subscribe(TOPIC_ALERTS)
        log.info(f'Subscribed to {TOPIC_SENSORS} and {TOPIC_ALERTS}')
    else:
        log.error(f'MQTT connect failed rc={rc}')

def on_disconnect(client, userdata, rc):
    log.warning(f'MQTT disconnected rc={rc}, reconnecting...')

def on_message(client, userdata, msg):
    topic = msg.topic
    try:
        payload = json.loads(msg.payload.decode('utf-8'))
    except Exception as e:
        log.error(f'JSON parse error on {topic}: {e}')
        return

    if topic == TOPIC_SENSORS:
        handle_sensor(payload)
    elif topic == TOPIC_ALERTS:
        handle_alert(payload)
    else:
        log.debug(f'Unhandled topic: {topic}')

def connect_mqtt():
    global mqtt_client
    try:
        import time
        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id=f'henhouse_bridge_{int(time.time())}')        c.username_pw_set(MQTT_USER, MQTT_PASS)
        c.on_connect    = on_connect
        c.on_disconnect = on_disconnect
        c.on_message    = on_message
        c.tls_set()
        c.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        c.loop_start()
        mqtt_client = c
        log.info('MQTT client started')
    except Exception as e:
        log.error(f'MQTT init failed: {e}')

connect_mqtt()

# ─────────────────────────────────────────────
# Keep-alive ping for Render free tier
# ─────────────────────────────────────────────
def keep_alive():
    while True:
        time.sleep(840)
        url = os.getenv('RENDER_EXTERNAL_URL', FLASK_URL)
        if url:
            try:
                requests.get(f'{url}/ping', timeout=10)
                log.info('Keep-alive ping sent')
            except Exception:
                pass

threading.Thread(target=keep_alive, daemon=True).start()

# ─────────────────────────────────────────────
# Auto-reconnect watchdog
# ─────────────────────────────────────────────
def watchdog():
    while True:
        time.sleep(60)
        if not MONGO_OK:
            log.warning('MongoDB offline, attempting reconnect...')
            connect_mongo()
        if mqtt_client and not mqtt_client.is_connected():
            log.warning('MQTT offline, attempting reconnect...')
            try:
                mqtt_client.reconnect()
            except Exception:
                connect_mqtt()

threading.Thread(target=watchdog, daemon=True).start()

# ─────────────────────────────────────────────
# Minimal Flask API (fallback for old HTML dashboard)
# ─────────────────────────────────────────────
from flask import Flask, jsonify, request as flask_request
from flask_cors import CORS

app = Flask(__name__)
CORS(app, origins='*')

@app.route('/ping')
def ping():
    return jsonify({'ok': True})

@app.route('/api/current')
def api_current():
    if not MONGO_OK:
        return jsonify({'error': 'DB unavailable'}), 503
    farm_id = flask_request.args.get('farm_id', 'henhouse_1')
    doc = sensors_col.find_one(
        {'$or': [{'farm_id': farm_id}, {'device_id': farm_id}]},
        sort=[('_id', DESCENDING)]
    )
    if not doc:
        return jsonify({'error': 'No data'}), 404
    doc['_id'] = str(doc['_id'])
    if 'created_at' in doc:
        doc['created_at'] = doc['created_at'].isoformat() if hasattr(doc['created_at'], 'isoformat') else str(doc['created_at'])
    return jsonify(doc)

@app.route('/api/history')
def api_history():
    if not MONGO_OK:
        return jsonify({'error': 'DB unavailable'}), 503
    farm_id = flask_request.args.get('farm_id', 'henhouse_1')
    limit   = min(int(flask_request.args.get('limit', 20)), 100)
    docs = list(sensors_col.find(
        {'$or': [{'farm_id': farm_id}, {'device_id': farm_id}]},
        sort=[('_id', DESCENDING)]
    ).limit(limit))
    for d in docs:
        d['_id'] = str(d['_id'])
        if 'created_at' in d:
            d['created_at'] = d['created_at'].isoformat() if hasattr(d['created_at'], 'isoformat') else str(d['created_at'])
    return jsonify(docs)

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5001))
    log.info(f'Bridge API running on port {port}')
    app.run(host='0.0.0.0', port=port)
