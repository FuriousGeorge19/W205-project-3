#!/usr/bin/env python
import json
from kafka import KafkaProducer
from flask import Flask, request

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')

def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())

@app.route("/")
def default_response():
    default_event = {'event_type': 'default'}
    log_to_kafka('events', default_event)
    return "This is the default response!\n"


@app.route("/purchase_a_sword/<sword_name>")
def purchase_a_sword(sword_name):
    purchase_sword_event = {'event_type': 'purchase_sword', 'sword_type': sword_name}
    log_to_kafka('events', purchase_sword_event)
    return "Sword Purchased!\n"

@app.route("/purchase_armor")
def purchase_armor():
    purchase_armor_event = {'event_type': 'purchase_armor'}
    log_to_kafka('events', purchase_armor_event)
    return "Armor Purchased!\n"

@app.route("/join_a_guild/<guild_name>")
def join_a_guild(guild_name):
    join_guild_event = {'event_type': 'join_a_guild','guild_type': guild_name}
    log_to_kafka('events', join_guild_event)
    return "Guild Joined!\n"
