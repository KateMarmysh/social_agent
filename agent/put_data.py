import pika
import json

f = open('data/groups.txt', "r")
groups = [line.strip() for line in f]
f.close()

f = open("data/users.txt", "r")
users = [line.strip() for line in f]
f.close()

connection_params = pika.ConnectionParameters(
    host="localhost",
    port=5672,
    virtual_host="/",
    credentials=pika.PlainCredentials(
        username="guest",
        password="guest"
    )
)

connection = pika.BlockingConnection(connection_params)
channel = connection.channel()

for group in groups:
    channel.basic_publish(exchange="social_data", routing_key="", body=json.dumps({"type": "group", "group_id": group}))
    f = open(f"data/group_{group}_subscribers.txt", "r")
    group_subscribers = [line.strip() for line in f]
    f.close()
    for user in group_subscribers:
        channel.basic_publish(exchange="social_data", routing_key="", body=json.dumps({"type": "subscriber", "group_id": group, "user_id": user}))

for user in users:
    channel.basic_publish(exchange="social_data", routing_key="", body=json.dumps({"type": "user", "user_id": user}))

channel.close()