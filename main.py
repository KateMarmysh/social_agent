#!/usr/local/bin/python3

from owlready2 import *
import pika
import vk_api
import model
import json
import event_miner

# https://oauth.vk.com/authorize?client_id=7230301&redirect_uri=https://localhost&response_type=code&scope=groups friends
# https://oauth.vk.com/access_token?client_id=7230301&client_secret=EfoAqkm3YcsIaRlRd7cq&code=683467eade23dd4136&redirect_uri=https://localhost
vk_session = vk_api.VkApi(token="b2159813ff701b26058f147c281de1bf871515be0428e80add2c2930290802050f7778cf7ae1c3644e6f8")
vk = vk_session.get_api()

target_users = []
target_groups = []


# Load person's friends from social network
def fill_friends(p):
    vk_friends_result = vk.friends.get(user_id=p.u_id)

    p.friends = vk_friends_result['items']


# Load person's group from social network
def fill_groups(p):
    vk_groups_result = vk.groups.get(user_id=p.u_id)

    p.groups = vk_groups_result['items']


# Get person from social network
def get_person(user_id):
    vk_user_result = vk.users.get(user_id=user_id,
                                  fields='bdate, photo_100, age, city, country, home_town, sex, games, online, domain, has_mobile, contacts, site, education, universities, schools, status, last_seen, followers_count, common_count, occupation, nickname, relatives, relation, personal, connections, exports, activities, interests, music, movies, tv, books, games, about, quotes, can_post, can_see_all_posts, can_see_audio')
    if len(vk_user_result) == 0:
        return
    vk_user = vk_user_result[0]
    photo = vk_user['photo_100'] if 'photo_100' in vk_user else 'unknown'
    gender = vk_user['sex'] if 'sex' in vk_user else 'unknown'
    birth_day = vk_user['bdate'] if 'bdate' in vk_user else 'unknown'
    person = model.Person(vk_user['id'], vk_user['first_name'], vk_user['last_name'], photo,
                          vk_user['domain'], gender=gender, birth_day=birth_day)
    fill_friends(person)
    fill_groups(person)

    return person


# Get community from social network
def get_community(group_id):
    vk_group_result = vk.groups.getById(group_id=group_id, fields='activity')
    if len(vk_group_result) == 0:
        return
    vk_group = vk_group_result[0]
    community = model.Community(vk_group['id'], vk_group['name'], vk_group['activity'])

    return community


def load_ontology():
    print("Loading ontology")
    onto_path.append("onto/")
    return get_ontology("file://onto/social-model.owl")


# Create or update ontology structure
def create_ontology():
    onto.load()
    with onto:
        class Person(Thing):
            pass

        class Community(Thing):
            pass

        class Interest(Thing):
            pass

        class Sports(Interest):
            pass

        class News(Interest):
            pass

        class Events(Interest):
            pass

        class JobSeekers(Interest):
            pass

        class Humor(Interest):
            pass

        class Politics(Interest):
            pass

        class Government(Interest):
            pass

        class Hobbies(Interest):
            pass

        class Books(Interest):
            pass

        class Programming(Interest):
            pass

        class Games(Interest):
            pass

        class Movies(Interest):
            pass

        class Music(Interest):
            pass

        class Health(Interest):
            pass

        class InformationTechnologies(Interest):
            pass

        class Travel(Interest):
            pass

        class Intention(Thing):
            pass

        class Communication(Intention):
            pass

        class Activity(Thing):
            pass

        class hasInterest(ObjectProperty):
            domain = [Person]
            range = [Sports, News, Events, Games, Politics, Health, Sports, Travel, InformationTechnologies, Government,
                     Movies, JobSeekers, Humor, Books, Hobbies, Music, Programming]

        class hasName(Community >> str):
            pass

        class hasFirstName(Person >> str):
            pass

        class hasLastName(Person >> str):
            pass

        class hasGender(Person >> str):
            pass

        class hasPhoto(Person >> str):
            pass

        class hasDomain(Person >> str):
            pass

        class hasBirthDay(Person >> str):
            pass

        class hasActivity(ObjectProperty):
            domain = [Person, Community]
            range = [Activity]
            pass

        class friendsWith(Person >> Person):
            pass

        class hasSubscriber(Community >> Person):
            pass

        class subscribedTo(ObjectProperty):
            domain = [Person]
            range = [Community]
            inverse_property = hasSubscriber
            pass
    onto.save()


# Create or update ontology person
def create_onto_person(person):
    onto.load()
    onto_person = onto.Person(str(person.u_id))
    onto_person.hasFirstName = [person.first_name]
    onto_person.hasLastName = [person.last_name]
    onto_person.hasGender = [person.gender]
    onto_person.hasBirthDay = [person.birth_day]
    onto_person.hasDomain = [person.domain]
    onto_person.hasPhoto = [person.photo]
    onto_person.hasActivity = []
    for friend in person.friends:
        if friend in target_users:
            onto_friend = onto.Person(str(friend))
            onto_person.friendsWith.append(onto_friend)
    for group in person.groups:
        if group in target_groups:
            onto_community = onto.Community(str(group))
            onto_person.subscribedTo.append(onto_community)
            if len(onto_community.hasActivity) > 0:
                onto_person.hasActivity.append(onto_community.hasActivity[0])
    onto.save()


# Create or update ontology community
def create_onto_community(community):
    onto_community = onto.Community(str(community.g_id))
    onto_community.hasName = [community.name]
    onto_community.hasActivity = [onto.Activity(community.activity)]


# Rabbit connection declare
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='social_data')
channel.exchange_declare(exchange='social_data_to_update')

channel.queue_declare(queue='social_data', durable=True)
channel.queue_declare(queue='social_data_to_update', durable=True,
                      arguments={
                          'x-message-ttl': 3 * 60 * 60 * 1000,  # 3 hours
                          "x-dead-letter-exchange": "social_data"
                      })

channel.queue_bind(exchange='social_data',
                   routing_key='',
                   queue='social_data')

channel.queue_bind(exchange='social_data_to_update',
                   routing_key='',
                   queue='social_data_to_update')

onto = load_ontology()
create_ontology()


# Rabbit new message callback
def callback(ch, method, properties, body):
    data = json.loads(body)

    if data['type'] == 'user':
        print("Processing user: %r" % data['user_id'])

        # Get person info from vk
        person = get_person(data['user_id'])

        # Check new user
        if person.u_id not in target_users:
            target_users.append(person.u_id)

            # Start mining events
            event_miner.UserEventMiner(person).start()

        # Update info in ontology
        create_onto_person(person)

        # Publish message to update entity after delay
        channel.basic_publish(exchange='social_data_to_update', routing_key='', body=body)

    if data['type'] == 'group':
        print("Processing group: %r" % data['group_id'])

        # Get community from vk
        community = get_community(data['group_id'])

        # Check new community
        if community.g_id not in target_groups:
            target_groups.append(community.g_id)

            # Start mining events
            event_miner.GroupEventMiner(community).start()

        # Update info in ontology
        create_onto_community(community)

        # Publish message to update entity after delay
        channel.basic_publish(exchange='social_data_to_update', routing_key='', body=body)


channel.basic_consume(queue='social_data', on_message_callback=callback, auto_ack=True)

consumer_thread = threading.Thread(target=channel.start_consuming)
consumer_thread.start()
print(' [*] Ontology consumer started\n')
print(' [*] To exit press CTRL+C')
