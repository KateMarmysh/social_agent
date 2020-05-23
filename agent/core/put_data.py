#!/usr/local/bin/python3.7

import pika
import json

# мехмат
f = open('data/users.txt', 'r')
users = [line.strip() for line in f]
f.close()

f = open('data/groups.txt', 'r')
groups = [line.strip() for line in f]
f.close()

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost"))
channel = connection.channel()

for group in groups:
    channel.basic_publish(exchange="social_data", routing_key="", body=json.dumps({"type": "group", "group_id": group}))

for user in users:
    channel.basic_publish(exchange="social_data", routing_key="", body=json.dumps({"type": "user", "user_id": user}))

for user in users:
    channel.basic_publish(exchange="social_data", routing_key="", body=json.dumps({"type": "user", "user_id": user}))
channel.close()
