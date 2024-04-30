from owlready2 import *
import vk_api
import copy
import json
import pika
import networkx as nx
from datetime import datetime, timezone
import threading
import random


class Ontology:
    def __init__(self, path):
        onto_path.append("web/onto/")
        self.path = path
        self.onto = get_ontology(self.path)
        self.onto_lock = threading.Lock()

    def create_ontology(self):
        self.onto.load()
        
        with self.onto:
            class Person(Thing):
                pass
                
            class hasFirstName(Person >> str):
                pass
                
            class hasLastName(Person >> str):
                pass
            
            class Community(Thing):
                pass
                
            class hasName(Community >> str):
                pass
            
            class Activity(Thing):
                pass

            class Post(Thing):
                pass
           
            class friendsWith(Person >> Person):
                pass
            
            class hasActivity(ObjectProperty):
                domain = [Community]
                range = [Activity]
                pass

            class subscribedTo(ObjectProperty):
                domain = [Person]
                range = [Community]
                pass
            
            class hasTag(Post >> str):
                pass
                            
            class viewedBy(Post >> Person):
                pass
            
            class likedBy(Post >> Person):
                pass

            class postedBy(ObjectProperty):
                domain = [Post]
                range = [Person, Community]
                pass
            
            class isRepostOf(Post >> Post):
                pass
            
            class repostedBy(ObjectProperty):
                domain = [Post]
                range = [Person, Community]
                pass

            class isAds(Post >> str):
                pass

            class hasDate(Post >> datetime):
                pass

        self.onto.save()

    def save_person(self, person):
        self.onto.load()
        onto_person = self.onto.Person(str(person.u_id))        
        onto_person.hasFirstName = [person.first_name]
        onto_person.hasLastName = [person.last_name]

        for friend in person.friends:
            onto_friend = self.onto.Person(str(friend))
            onto_person.friendsWith.append(onto_friend)
            onto_friend.friendsWith.append(onto_person)
        for group in person.groups:
            onto_community = self.onto.Community(str(group))
            onto_person.subscribedTo.append(onto_community)
        self.onto.save()

    def save_community(self, community):
        self.onto.load()    
        onto_community = self.onto.Community(str(community.g_id))
        onto_community.hasName = [community.name]
        onto_community.hasActivity = [self.onto.Activity(community.activity)]
        self.onto.save()

    def save_post(self, post):
        self.onto_lock.acquire()
        self.onto.load()
        onto_post = self.onto.Post(str(post.id))
        onto_post.postedBy = []
        onto_post.repostedBy = []
        onto_post.isRepostOf = []
        onto_post.viewedBy = []
        onto_post.likedBy = []
        onto_post.hasTag = []

        # Check owner community or person
        if "-" in str(post.owner_id):
            onto_post.postedBy.append(self.onto.Community(str(post.owner_id)[1:]))
        else:
            onto_post.postedBy.append(self.onto.Person(str(post.owner_id)))
                    
        for tag in post.tags:
            onto_post.hasTag.append(tag)
 
        onto_post.isAds = [post.is_ads]
        onto_post.hasDate = [datetime.fromtimestamp(post.date)]
        
        for repost_id in post.copy_history:
            reposted_post = self.onto.Post(str(repost_id))
            onto_post.isRepostOf.append(reposted_post)
            if "-" in str(post.owner_id):
                reposted_post.repostedBy.append(self.onto.Community(str(post.owner_id)[1:]))
            else:
                reposted_post.repostedBy.append(self.onto.Person(str(post.owner_id)))
                reposted_post.viewedBy.append(self.onto.Person(str(post.owner_id)))
                reposted_post.likedBy.append(self.onto.Person(str(post.owner_id)))

        self.onto.save()
        self.onto_lock.release()
    
    def post_viewed(self, user_id, post):
        self.onto_lock.acquire()
        self.onto.load()
        self.onto.Post(str(post.id)).viewedBy.append(self.onto.Person(str(user_id)))
        self.onto.save()
        self.onto_lock.release()

    def post_liked(self, user_id, post):
        self.onto_lock.acquire()
        self.onto.load()
        self.onto.Post(str(post.id)).likedBy.append(self.onto.Person(str(user_id)))
        self.onto.save()
        self.onto_lock.release()


class VK:
    def __init__(self, token, target_users, target_groups, goups_subscribers):
        self.api = vk_api.VkApi(token=token).get_api()
        self.target_users = target_users
        self.target_groups = target_groups
        self.goups_subscribers = goups_subscribers        
        self.posts = []


    # Get person from social network
    def get_person(self, user_id):
        try:
            vk_user_result = self.api.users.get(user_id=user_id)
        except Exception as er:
            print(f"{user_id}: {er}")
            time.sleep(random.randint(1, 60))
            return
        
        if len(vk_user_result) == 0:
            return
        
        vk_user = vk_user_result[0]
        
        friends = self.get_person_friends(user_id)
        groups = self.get_person_groups(user_id)
        
        person = Person(vk_user["id"], vk_user["first_name"], vk_user["last_name"], friends=friends, groups=groups)

        return person

    # Load person"s friends from social network
    def get_person_friends(self, user_id):
        vk_friends_result = None
        
        try:
            vk_friends_result = self.api.friends.get(user_id=user_id)
        except Exception as er:
            print(f"{user_id}: {er}")
            time.sleep(random.randint(1, 60))
            return []

        friends = []
        
        for friend in vk_friends_result["items"]:
            if friend in self.target_users:
                friends.append(friend)

        return friends

    # Load person"s group from social network
    def get_person_groups(self, user_id):
        groups = []
        
        for group in self.target_groups:
            if user_id in self.goups_subscribers[group]:
                groups.append(group)

        return groups

    # Get community from social network
    def get_community(self, group_id):
        try:
            vk_group_result = self.api.groups.getById(group_id=group_id, fields="activity")
        except Exception as er:
            print(f"{group_id}: {er}")
            time.sleep(random.randint(1, 60))
            return
        
        if len(vk_group_result) == 0:
            return

        vk_group = vk_group_result[0]
        community = Community(vk_group["id"], vk_group["name"], vk_group["activity"])

        return community
    

    # Get posts from social network
    def get_posts(self, owner_id, count, last_post_id):
        posts = []

        try:
            items = self.api.wall.get(owner_id=owner_id, count=count)["items"]
        except Exception as er:
            print(f"{owner_id}: {er}")
            time.sleep(random.randint(1, 60))
            return []

        for post in items:
            if "is_pinned" in post and post["is_pinned"]:
                continue

            # Get only last day posts
            if (datetime.now() - datetime.fromtimestamp(post["date"])).days > 1:
                break

            p_id = str(owner_id) + "_" + str(post["id"])

            if p_id == last_post_id:
                break

            tags = [tag.strip("#") for tag in post["text"].split() if tag.startswith("#")]

            is_ads = 0
            if "marked_as_ads" in post and post["marked_as_ads"] == 1:
                is_ads = 1

            new_post = Post(p_id, owner_id, post["date"], post["post_type"], is_ads, tags)

            if "copy_history" in post and len(post["copy_history"]) > 0:
                for repost in post["copy_history"]:
                    new_post.add_history(str(repost["owner_id"]) + "_" + str(repost["id"]))

            if new_post not in self.posts:
                self.posts.append(new_post)

            posts.append(new_post)

        return posts

    def get_liked_posts(self, person):
        liked_posts = []

        for post in self.posts:
            try:
                liked_post = self.api.likes.isLiked(user_id=person.u_id,
                                                    owner_id=post.id.split("_")[0],
                                                    item_id=post.id.split("_")[1],
                                                    type=post.type)
            except:
                time.sleep(random.randint(1, 60))
                continue

            if not (post in person.posts_liked) and liked_post["liked"]:
                liked_posts.append(post)

        return liked_posts

    def is_user_online(self, person):
        try:
            user = self.api.users.get(user_id=person.u_id, fields="online")[0]
        except Exception as er:
           print(f"{person.u_id}: {er}")
           time.sleep(random.randint(1, 60))
           return

        return user["online"] == 1


class NewsFeed:
    def __init__(self):
        self.global_news_feed = {}

    def is_empty(self, owner_id):
        return owner_id not in self.global_news_feed or len(self.global_news_feed[owner_id]["items"]) == 0

    def get_last_post_id(self, owner_id):
        if self.is_empty(owner_id):
            return -1
        
        return self.global_news_feed[owner_id]["last_post"]

    def add_posts(self, owner_id, posts):
        if len(posts) == 0:
            return

        if self.is_empty(owner_id):
            self.global_news_feed[owner_id] = {
                "last_post": posts[0].id,
                "lock": threading.Lock(),
                "items": posts
            }
            return

        self.global_news_feed[owner_id]["lock"].acquire()
        try:
            self.global_news_feed[owner_id]["last_post"] = posts[0].id
            self.global_news_feed[owner_id]["items"] = posts + self.global_news_feed[owner_id]["items"]
        finally:
            self.global_news_feed[owner_id]["lock"].release()

    def get_user_news(self, person):
        news_feed = []

        for group_id in person.groups:
            group_id = "-" + str(group_id)
            if group_id in self.global_news_feed:
                self.global_news_feed[group_id]["lock"].acquire()
                try:
                    news_feed += self.global_news_feed[group_id]["items"]
                finally:
                    self.global_news_feed[group_id]["lock"].release()

        for friend_id in person.friends:
            if friend_id in self.global_news_feed:
                self.global_news_feed[friend_id]["lock"].acquire()
                try:
                    news_feed += self.global_news_feed[friend_id]["items"]
                finally:
                    self.global_news_feed[friend_id]["lock"].release()

        return sorted(news_feed, key=lambda p: p.date, reverse=True)


class Person:
    def __init__(self, u_id, first_name, last_name, friends, groups):
        self.u_id = u_id
        self.first_name = first_name
        self.last_name = last_name
        self.friends = friends
        self.groups = groups
        self.posts_seen = []
        self.posts_liked = []


class Community:
    def __init__(self, g_id, name, activity):
        self.g_id = g_id
        self.name = name
        self.activity = activity


class Post:
    def __init__(self, id, owner_id, date, type, is_ads, tags):
        self.id = id
        self.owner_id = owner_id
        self.date = date
        self.type = type
        self.is_ads = is_ads
        self.tags = tags
        self.copy_history = []

    def add_history(self, post_id):
        self.copy_history.append(post_id)

    def is_reposted(self):
        return len(self.copy_history) > 0


class GroupEventMiner(threading.Thread):
    def __init__(self, vk, news_feed, ontology, community):
        threading.Thread.__init__(self)
        self.community = community
        self.vk = vk
        self.news_feed = news_feed
        self.ontology = ontology
        self.owner_id = "-" + str(self.community.g_id)

    def run(self):
        print("start mining events for community %s" % self.community.g_id)

        time_now = 0
        posts = []

        while True:
            if time_now % time_to_update_news == 0:
                self.load_news_feed()
            
            time_now += time_tick
            time.sleep(time_tick)

    def load_news_feed(self):
        posts = self.vk.get_posts(self.owner_id, posts_count, self.news_feed.get_last_post_id(self.owner_id))

        for post in posts:
            self.ontology.save_post(post)
        
        self.news_feed.add_posts(self.owner_id, posts)


class UserEventMiner(threading.Thread):
    def __init__(self, vk, news_feed, ontology, person):
        threading.Thread.__init__(self)
        self.vk = vk
        self.news_feed = news_feed
        self.ontology = ontology
        self.person = person
        self.owner_id = person.u_id
        self.user_online = False

    def run(self):
        print("start mining events for user %s" % self.person.u_id)

        time_now = 0
        post_idx = 0
        posts = []

        while True:
            # Check user likes and reposts
            if time_now % time_to_check_likes == 0:
                liked_posts = self.vk.get_liked_posts(self.person)

                for liked_post in liked_posts:
                    self.person.posts_liked.append(liked_post)
                    self.ontology.post_liked(self.person.u_id, liked_post)
                    
                    self.person.posts_seen.append(liked_post)
                    self.ontology.post_viewed(self.person.u_id, liked_post)

            # Check user status
            if time_now % time_to_check_online == 0:
                is_online = self.vk.is_user_online(self.person)
                if not self.user_online and is_online:
                    self.user_online = True
                elif self.user_online and not is_online:
                    self.user_online = False
                    time_now = 0

            # Load user news feed
            if time_now % time_to_update_user_news == 0:
                posts = self.news_feed.get_user_news(self.person)
                post_idx = 0

            # Check user see new post
            if self.user_online and time_now % time_to_see_post == 0:
                if post_idx < len(posts):
                    self.person.posts_seen.append(posts[post_idx])
                    self.ontology.post_viewed(self.person.u_id, posts[post_idx])
                    post_idx += 1

            # Load or update news from person
            if time_now % time_to_update_news == 0:
                self.load_news_feed()

            time_now += time_tick
            time.sleep(time_tick)
    
    def load_news_feed(self):
        posts = self.vk.get_posts(self.owner_id, posts_count, self.news_feed.get_last_post_id(self.owner_id))

        for post in posts:
            self.ontology.save_post(post)

        self.news_feed.add_posts(self.owner_id, posts)


class NetworkAnalyzer:
    G = nx.Graph()

    def add_node(self, person):
        self.G.add_node(person.u_id)
        for friend in person.friends:
            self.G.add_node(friend)
            self.G.add_edge(person.u_id, friend)

    def print_network_characteristics(self):
        time.sleep(time_to_analyze_network)
        print(f"Network Analyzer report:\n\
        Degree centrality: {nx.degree_centrality(self.G)}\n\
        Eigenvector centrality: {nx.eigenvector_centrality(self.G)}\n\
        Closeness centrality: {nx.closeness_centrality(self.G)}\n\
        Betweenness centrality: {nx.betweenness_centrality(self.G)}")


def callback(ch, method, properties, body):
    data = json.loads(body)

    if data["type"] == "user":
        print("Processing user: %r" % data["user_id"])

        # Get person info from vk
        person = vk.get_person(data["user_id"])
        if person.u_id not in target_users:
            vk.target_users.append(person.u_id)
            network_analyzer.add_node(person)

            # Start mining events
            UserEventMiner(vk, news_feed, ontology, person).start()

        # Update info in ontology
        ontology.save_person(person)
    
    elif data["type"] == "group":
        print("Processing group: %r" % data["group_id"])

        # Get community from vk
        community = vk.get_community(data["group_id"])
        if community.g_id not in target_groups:
            vk.target_groups.append(community.g_id)
            
            # Start mining events
            GroupEventMiner(vk, news_feed, ontology, community).start()

        # Update info in ontology
        ontology.save_community(community)
        
    elif data["type"] == "subscriber":
        # Upload goups subscribers
        if int(data["group_id"]) not in vk.goups_subscribers:
            vk.goups_subscribers[int(data["group_id"])] = []
            vk.goups_subscribers[int(data["group_id"])].append(data["user_id"])
        else:
            vk.goups_subscribers[int(data["group_id"])].append(data["user_id"])


def connect_to_rabbitmq():
    while True:
        try:
            connection = pika.BlockingConnection(connection_params)
            print("Successfully connected to RabbitMQ")
            return connection
        except pika.exceptions.AMQPConnectionError:
            print("Connection error, retrying...")
            continue


def setup_rabbitmq():
    connection = connect_to_rabbitmq()
    channel = connection.channel()
    channel.exchange_declare(exchange="social_data")
    channel.queue_declare(queue="social_data", durable=True)
    channel.queue_bind(exchange="social_data", routing_key="", queue="social_data")
    return connection, channel


def consume_messages(connection, channel):
    while True:
        try:
            channel.basic_consume(queue="social_data", on_message_callback=callback, auto_ack=True)
            print("\n[*] Ontology consumer in process\n")
            channel.start_consuming()
        except pika.exceptions.StreamLostError as e:
            print(f"\n{e}")
            print("Reconnecting to RabbitMQ...\n")
            if connection.is_open:
                connection.close()
            connection, channel = setup_rabbitmq()


posts_count = int(os.getenv("SA_POSTS_COUNT"))
time_tick = int(os.getenv("SA_TIME_TICK"))
time_to_analyze_network = int(os.getenv("SA_TIME_TO_ANALYZE_NETWORK"))
time_to_see_post = int(os.getenv("SA_TIME_TO_SEE_POST"))
time_to_check_likes = int(os.getenv("SA_TIME_TO_CHECK_LIKES"))
time_to_check_online = int(os.getenv("SA_TIME_TO_CHECK_ONLINE"))
time_to_update_news = int(os.getenv("SA_TIME_TO_UPDATE_NEWS"))
time_to_update_user_news = int(os.getenv("SA_TIME_TO_UPDATE_USER_NEWS"))

file_path = f"onto/social-model_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.owl"
if not os.path.exists(file_path):
    open(file_path, 'w').close()

ontology = Ontology(f"file://onto/social-model_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.owl")  
ontology.create_ontology()

target_users = []
target_groups = []
goups_subscribers = {}

vk = VK(os.getenv("VK_API_TOKEN"), target_users, target_groups, goups_subscribers)

news_feed = NewsFeed()
network_analyzer = NetworkAnalyzer()

connection_params = pika.ConnectionParameters(
    host='localhost',
    port=5672,
    virtual_host='/',
    credentials=pika.PlainCredentials(
        username='guest',
        password='guest'
    )
)

connection, channel = setup_rabbitmq()


# Start analyzer writer thread
analyzer_writer_thread = threading.Thread(target=network_analyzer.print_network_characteristics)
analyzer_writer_thread.start()

# Start consumer thread
consumer_thread = threading.Thread(target=consume_messages(connection, channel))
consumer_thread.start()