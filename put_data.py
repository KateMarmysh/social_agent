import pika
import json


users = [53182060, 44239068, 45388386, 361950485, 235995129, 132549939, 135282929, 80817183, 90271290, 166286700]

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

for user in users:
    channel.basic_publish(exchange='social_data', routing_key='', body=json.dumps({'type': 'user', 'user_id': user}))
channel.close()
