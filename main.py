#!/usr/local/bin/python3

from owlready2 import *
import pika
import vk_api
import model
import json
import event_miner
import os

# https://oauth.vk.com/authorize?client_id=7230301&redirect_uri=https://localhost&response_type=code&scope=groups friends
# https://oauth.vk.com/access_token?client_id=7230301&client_secret=EfoAqkm3YcsIaRlRd7cq&code=683467eade23dd4136&redirect_uri=https://localhost

target_users = []
target_groups = []

ontology = model.Ontology("file://onto/social-model.owl")
ontology.create_ontology()
vk = model.VK(os.getenv("VK_API_TOKEN"), target_users, target_groups)
event_log = model.EventLog("./logs")
news_feed = model.NewsFeed()

# Rabbit connection declare
connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()

channel.exchange_declare(exchange="social_data")
channel.exchange_declare(exchange="social_data_to_update")

channel.queue_declare(queue="social_data", durable=True)
channel.queue_declare(queue="social_data_to_update", durable=True,
                      arguments={
                          "x-message-ttl": 3 * 60 * 60 * 1000,  # 3 hours
                          "x-dead-letter-exchange": "social_data"
                      })

channel.queue_bind(exchange="social_data",
                   routing_key="",
                   queue="social_data")

channel.queue_bind(exchange="social_data_to_update",
                   routing_key="",
                   queue="social_data_to_update")


# Rabbit new message callback
def callback(ch, method, properties, body):
    data = json.loads(body)

    if data["type"] == "user":
        print("Processing user: %r" % data["user_id"])

        # Get person info from vk
        person = vk.get_person(data["user_id"])

        # Check new user
        if person.u_id not in target_users:
            vk.target_users.append(person.u_id)

            # Start mining events
            event_miner.UserEventMiner(vk, news_feed, ontology, event_log, person).start()

        # Update info in ontology
        ontology.save_person(person)

        # Publish message to update entity after delay
        channel.basic_publish(exchange="social_data_to_update", routing_key="", body=body)

    if data["type"] == "group":
        print("Processing group: %r" % data["group_id"])

        # Get community from vk
        community = vk.get_community(data["group_id"])

        # Check new community
        if community.g_id not in target_groups:
            vk.target_groups.append(community.g_id)

            # Start mining events
            event_miner.GroupEventMiner(vk, news_feed, ontology, event_log, community).start()

        # Update info in ontology
        ontology.save_community(community)

        # Publish message to update entity after delay
        channel.basic_publish(exchange="social_data_to_update", routing_key="", body=body)


channel.basic_consume(queue="social_data", on_message_callback=callback, auto_ack=True)

consumer_thread = threading.Thread(target=channel.start_consuming)
consumer_thread.start()
print(" [*] Ontology consumer started\n")


def write_log():
    while True:
        event_log.write_log()
        time.sleep(60)


log_writer_thread = threading.Thread(target=write_log)
log_writer_thread.start()
print(" [*] To exit press CTRL+C")
