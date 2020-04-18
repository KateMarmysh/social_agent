#!/usr/local/bin/python3

import vk_api
import threading
import time
import xes
import threading
from datetime import datetime, timezone
import copy

# https://oauth.vk.com/authorize?client_id=7230301&redirect_uri=https://localhost&response_type=code&scope=friends groups offline
# https://oauth.vk.com/access_token?client_id=7230301&client_secret=EfoAqkm3YcsIaRlRd7cq&code=683467eade23dd4136&redirect_uri=https://localhost
vk_session = vk_api.VkApi(token="b2159813ff701b26058f147c281de1bf871515be0428e80add2c2930290802050f7778cf7ae1c3644e6f8")
vk = vk_session.get_api()

posts_count = 15
time_tick = 15
time_to_see_post = 15
time_to_check_likes = 1 * 60
time_to_check_online = 1 * 60  # 1 min
time_to_update_news = 3 * 60  # 5 min
time_to_update_user_news = 3 * 60  # 3 min

global_news_feed = {}
news_feed_lock = threading.Lock()

log = xes.Log()
log_lock = threading.Lock()
log.classifiers = [
    xes.Classifier(name="org:resource", keys="org:resource"),
    xes.Classifier(name="concept:name", keys="concept:name")
]


class GroupEventMiner(threading.Thread):
    def __init__(self, community):
        threading.Thread.__init__(self)
        self.community = community
        self.trace = xes.Trace()

    def run(self):
        print("Mining events for community %s" % self.community.g_id)

        time_now = 0
        post_idx = 0
        posts = []
        posts_seen = {}

        while True:
            # Load or update news from community
            if time_now % time_to_update_news == 0:
                if self.community.g_id not in global_news_feed:
                    self.load_news_feed()
                else:
                    self.update_news_feed()

                time_now = 0

            # time tick
            time_now += time_tick
            time.sleep(time_tick)

    def update_news_feed(self):
        if self.community.g_id not in global_news_feed or len(global_news_feed[self.community.g_id]['items']) == 0:
            return

        news_feed = []

        try:
            posts = vk.wall.get(owner_id="-" + str(self.community.g_id), count=time_to_update_news // 60)["items"]
        except:
            return

        for post in posts:
            if "is_pinned" in post and post["is_pinned"]:
                continue

            if (datetime.now() - datetime.fromtimestamp(post["date"])).days > 1:
                break

            if 'copy_history' in post and len(post['copy_history']) > 0:
                p_id = str(post['copy_history'][0]['owner_id']) + "_" + str(post['copy_history'][0]['id'])
            else:
                p_id = "-" + str(self.community.g_id) + "_" + str(post["id"])

            if p_id == global_news_feed[self.community.g_id]['last_post']:
                break

            news_feed.append({
                "id": p_id,
                "owner_id": self.community.g_id,
                "date": post["date"],
                "type": post["post_type"],
                "marked_as_ads": post["marked_as_ads"]
            })

        if len(news_feed) == 0:
            return

        global_news_feed[self.community.g_id]['lock'].acquire()
        try:
            global_news_feed[self.community.g_id]['last_post'] = news_feed[0]['id']
            global_news_feed[self.community.g_id]['items'] = news_feed + global_news_feed[self.community.g_id]['items']
        finally:
            global_news_feed[self.community.g_id]['lock'].release()

    def load_news_feed(self):
        news_feed = []

        try:
            posts = vk.wall.get(owner_id="-" + str(self.community.g_id), count=posts_count)["items"]
        except:
            return

        for post in posts:
            if "is_pinned" in post and post["is_pinned"]:
                continue

            if (datetime.now() - datetime.fromtimestamp(post["date"])).days > 1:
                break

            if 'copy_history' in post and len(post['copy_history']) > 0:
                p_id = str(post['copy_history'][0]['owner_id']) + "_" + str(post['copy_history'][0]['id'])
            else:
                p_id = "-" + str(self.community.g_id) + "_" + str(post["id"])

            news_feed.append({
                "id": p_id,
                "owner_id": self.community.g_id,
                "date": post["date"],
                "type": post["post_type"],
                "marked_as_ads": post["marked_as_ads"]
            })

        if len(news_feed) == 0:
            return

        news_feed_lock.acquire()
        try:
            global_news_feed[self.community.g_id] = {
                'last_post': news_feed[0]['id'],
                'lock': threading.Lock(),
                'items': news_feed
            }
        finally:
            news_feed_lock.release()


class UserEventMiner(threading.Thread):
    def __init__(self, person):
        threading.Thread.__init__(self)
        self.person = person
        self.user_online = False
        self.trace = None
        self.posts_seen = {}

    def run(self):
        print("Mining events for user %s" % self.person.u_id)

        time_now = 0
        post_idx = 0
        posts = []

        while True:
            # Check user likes and reposts
            if self.user_online and time_now % time_to_check_likes == 0:
                liked_posts = self.get_liked_posts()

                for liked_post in liked_posts['liked']:
                    self.trace.add_event(event_post_liked(self.person, liked_post))
                    self.posts_seen[liked_post['id']]['post_liked'] = True

                for liked_post in liked_posts['copied']:
                    self.trace.add_event(event_post_copied(self.person, liked_post))
                    self.posts_seen[liked_post['id']]['post_copied'] = True

            # Check user status
            if time_now % time_to_check_online == 0:
                user = vk.users.get(user_id=self.person.u_id, fields='online')[0]
                if not self.user_online and user['online'] == 1:
                    print("User %s online" % self.person.u_id)
                    self.trace = xes.Trace()
                    self.trace.add_event(event_online(self.person))
                    self.user_online = True
                elif self.user_online and user['online'] == 0:
                    print("User %s offline" % self.person.u_id)
                    self.trace.add_event(event_offline(self.person))
                    add_trace(self.trace)
                    self.user_online = False
                    time_now = 0

                    # Write data to log
                    print("Writing data to log")
                    write_log()

            # Load user news feed
            if self.user_online and time_now % time_to_update_user_news == 0:
                time.sleep(10)
                posts = self.get_user_news()
                post_idx = 0

            # Check user see new post
            if self.user_online and time_now % (time_to_see_post * (len(self.person.groups) // 30 + 1)) == 0:
                if post_idx < len(posts):
                    self.trace.add_event(event_post_seen(self.person, posts[post_idx]))
                    self.posts_seen[posts[post_idx]['id']] = posts[post_idx]
                    post_idx += 1

            # Load or update news from person
            if time_now % time_to_update_news == 0:
                if self.person.u_id not in global_news_feed:
                    self.load_news_feed()
                else:
                    self.update_news_feed()

            # time tick
            time_now += time_tick
            time.sleep(time_tick)

    def get_user_news(self):
        news_feed = []

        for group_id in self.person.groups:
            if group_id in global_news_feed:
                global_news_feed[group_id]['lock'].acquire()
                try:
                    news_feed += global_news_feed[group_id]['items']
                finally:
                    global_news_feed[group_id]['lock'].release()

        for friend_id in self.person.friends:
            if friend_id in global_news_feed:
                global_news_feed[friend_id]['lock'].acquire()
                try:
                    news_feed += global_news_feed[friend_id]['items']
                finally:
                    global_news_feed[friend_id]['lock'].release()

        return sorted(news_feed, key=lambda p: p['date'], reverse=True)

    def get_liked_posts(self):
        liked_posts = {'liked': [], 'copied': []}

        for post_id in self.posts_seen:
            try:
                liked_post = vk.likes.isLiked(user_id=self.person.u_id,
                                              owner_id=post_id.split('_')[0],
                                              item_id=post_id.split('_')[1],
                                              type=self.posts_seen[post_id]['type'])
            except:
                continue

            if not ('post_liked' in self.posts_seen[post_id] and self.posts_seen[post_id]['post_liked']) \
                    and liked_post['liked']:
                liked_posts['liked'].append(self.posts_seen[post_id])

            if not ('post_copied' in self.posts_seen[post_id] and self.posts_seen[post_id]['post_copied']) and \
                    liked_post['copied']:
                liked_posts['copied'].append(self.posts_seen[post_id])

        return liked_posts

    def update_news_feed(self):
        if self.person.u_id not in global_news_feed:
            return

        news_feed = []

        try:
            posts = vk.wall.get(owner_id=str(self.person.u_id), count=time_to_update_news // 60)["items"]
        except:
            return

        for post in posts:
            if "is_pinned" in post and post["is_pinned"]:
                continue

            if post["id"] == global_news_feed[self.person.u_id]['last_post']:
                break

            if (datetime.now() - datetime.fromtimestamp(post["date"])).days > 1:
                break

            if 'copy_history' in post and len(post['copy_history']) > 0:
                p_id = str(post['copy_history'][0]['owner_id']) + "_" + str(post['copy_history'][0]['id'])
            else:
                p_id = str(self.person.u_id) + "_" + str(post["id"])

            if p_id == global_news_feed[self.person.u_id]['last_post']:
                break

            news_feed.append({
                "id": p_id,
                "owner_id": self.person.u_id,
                "date": post["date"],
                "type": post["post_type"],
                "marked_as_ads": 0
            })

        if len(news_feed) == 0:
            return

        global_news_feed[self.person.u_id]['lock'].acquire()
        try:
            global_news_feed[self.person.u_id]['last_post'] = news_feed[0]['id']
            global_news_feed[self.person.u_id]['items'] = news_feed + global_news_feed[self.person.u_id]['items']
        finally:
            global_news_feed[self.person.u_id]['lock'].release()

    def load_news_feed(self):
        news_feed = []

        try:
            posts = vk.wall.get(owner_id=str(self.person.u_id), count=posts_count)["items"]
        except:
            return

        if len(posts) == 0:
            return

        for post in posts:
            if "is_pinned" in post and post["is_pinned"]:
                continue

            if (datetime.now() - datetime.fromtimestamp(post["date"])).days > 1:
                break

            if 'copy_history' in post and len(post['copy_history']) > 0:
                p_id = str(post['copy_history'][0]['owner_id']) + "_" + str(post['copy_history'][0]['id'])
            else:
                p_id = str(self.person.u_id) + "_" + str(post["id"])

            news_feed.append({
                "id": p_id,
                "owner_id": self.person.u_id,
                "date": post["date"],
                "type": post["post_type"],
                "marked_as_ads": 0
            })

        if len(news_feed) == 0:
            return

        news_feed_lock.acquire()
        try:
            global_news_feed[self.person.u_id] = {
                'last_post': news_feed[0]['id'],
                'lock': threading.Lock(),
                'items': news_feed
            }
        finally:
            news_feed_lock.release()


def add_trace(trace):
    log_lock.acquire()
    try:
        log.add_trace(trace)
    finally:
        log_lock.release()


def write_log():
    log_lock.acquire()
    try:
        copy_log = copy.deepcopy(log)
        with open("./logs/log_4.xes", 'w') as filetowrite:
            filetowrite.write(str(copy_log))
            filetowrite.close()
    finally:
        log_lock.release()


def event_online(person):
    e = xes.Event()
    e.attributes = [
        xes.Attribute(type="string", key="concept:name", value="online"),
        xes.Attribute(type="string", key="org:resource", value=str(person.u_id)),
        xes.Attribute(type="string", key="user:name", value=str(person.u_id)),
        xes.Attribute(type="date", key="time:timestamp", value=datetime.now(timezone.utc).isoformat())
    ]
    return e


def event_offline(person):
    e = xes.Event()
    e.attributes = [
        xes.Attribute(type="string", key="concept:name", value="offline"),
        xes.Attribute(type="string", key="org:resource", value=str(person.u_id)),
        xes.Attribute(type="date", key="time:timestamp", value=datetime.now(timezone.utc).isoformat())
    ]
    return e


def event_post_seen(person, post):
    e = xes.Event()
    e.attributes = [
        xes.Attribute(type="string", key="concept:name", value="post_seen"),
        xes.Attribute(type="string", key="org:resource", value=str(person.u_id)),
        xes.Attribute(type="string", key="post:id", value=str(post['id'])),
        xes.Attribute(type="string", key="post:type", value=str(post['type'])),
        xes.Attribute(type="string", key="post:is_ads", value=str(post['marked_as_ads'])),
        xes.Attribute(type="date", key="post:date",
                      value=datetime.fromtimestamp(post['date']).astimezone(timezone.utc).isoformat()),
        xes.Attribute(type="string", key="owner:id", value=str(post['owner_id'])),
        xes.Attribute(type="date", key="time:timestamp", value=datetime.now(timezone.utc).isoformat())
    ]
    return e


def event_post_liked(person, post):
    e = xes.Event()
    e.attributes = [
        xes.Attribute(type="string", key="concept:name", value="post_liked"),
        xes.Attribute(type="string", key="org:resource", value=str(person.u_id)),
        xes.Attribute(type="string", key="post:id", value=str(post['id'])),
        xes.Attribute(type="string", key="post:type", value=str(post['type'])),
        xes.Attribute(type="string", key="post:is_ads", value=str(post['marked_as_ads'])),
        xes.Attribute(type="date", key="post:date",
                      value=datetime.fromtimestamp(post['date']).astimezone(timezone.utc).isoformat()),
        xes.Attribute(type="string", key="owner:id", value=str(post['owner_id'])),
        xes.Attribute(type="date", key="time:timestamp", value=datetime.now(timezone.utc).isoformat())
    ]
    return e


def event_post_copied(person, post):
    e = xes.Event()
    e.attributes = [
        xes.Attribute(type="string", key="concept:name", value="post_copied"),
        xes.Attribute(type="string", key="org:resource", value=str(person.u_id)),
        xes.Attribute(type="string", key="post:id", value=str(post['id'])),
        xes.Attribute(type="string", key="post:type", value=str(post['type'])),
        xes.Attribute(type="string", key="post:is_ads", value=str(post['marked_as_ads'])),
        xes.Attribute(type="date", key="post:date",
                      value=datetime.fromtimestamp(post['date']).astimezone(timezone.utc).isoformat()),
        xes.Attribute(type="string", key="owner:id", value=str(post['owner_id'])),
        xes.Attribute(type="date", key="time:timestamp", value=datetime.now(timezone.utc).isoformat())
    ]
    return e
