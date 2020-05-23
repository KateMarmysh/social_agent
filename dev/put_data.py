#!/usr/local/bin/python3

import pika
import json
import time

# мехмат
users = [53182060, 361950485, 132549939, 125749375, 199012007, 235995129, 85185902, 102156006, 44239068, 80817183,
         282875460, 54845968, 82156740, 32859585, 45388386, 93900513, 222492086]

u = [53182060]

groups = [186468555, 3551694, 59218056, 60397113, 151288610, 110581128, 40202469, 30210603, 46038605, 7557592]

g = [31480508]

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
