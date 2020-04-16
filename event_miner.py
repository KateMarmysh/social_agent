#!/usr/local/bin/python3

import vk_api
import threading
import time
import xes
import threading
import datetime
import copy

# https://oauth.vk.com/authorize?client_id=7230301&redirect_uri=https://localhost&response_type=code&scope=friends groups offline
# https://oauth.vk.com/access_token?client_id=7230301&client_secret=EfoAqkm3YcsIaRlRd7cq&code=683467eade23dd4136&redirect_uri=https://localhost
vk_session = vk_api.VkApi(token="b2159813ff701b26058f147c281de1bf871515be0428e80add2c2930290802050f7778cf7ae1c3644e6f8")
vk = vk_session.get_api()

# posts = vk.wall.get(owner_id="-31480508", count=1)#, fields='bdate, photo_100, city, country, home_town, sex, games, online, domain, has_mobile, contacts, site, education, universities, schools, status, last_seen, followers_count, common_count, occupation, nickname, relatives, relation, personal, connections, exports, activities, interests, music, movies, tv, books, games, about, quotes, can_post, can_see_all_posts, can_see_audio')

# print(vk.groups.getById(group_id=20629724, fields='activity'))

posts_count = 50
time_to_see_post = 15
time_to_check_likes = 5 * 60
time_to_check_online = 5 * 60  # 5 min
time_to_update_news = 10 * 60  # 10 min
time_to_write_log = 5 * 60


class UserEventMiner(threading.Thread):
    def __init__(self, user, group_ids):
        threading.Thread.__init__(self)
        self.user = user
        self.group_ids = group_ids
        self.trace = xes.Trace()

    def run(self):
        print("User %s online" % self.user['id'])
        self.trace.add_event(event_online(self.user))
        time_now = 0
        post_idx = 0
        posts = []
        posts_seen = {}
        while True:
            if time_now % time_to_update_news == 0:
                posts = load_news_feed(self.group_ids)
                post_idx = 0
            if time_now % time_to_check_likes == 0:
                liked_posts = load_liked_posts(self.user['id'], posts_seen)
                for liked_post in liked_posts['liked']:
                    self.trace.add_event(event_post_liked(self.user, liked_post))
                    posts_seen[liked_post['id']]['post_liked'] = True
                for liked_post in liked_posts['copied']:
                    self.trace.add_event(event_post_copied(self.user, liked_post))
                    posts_seen[liked_post['id']]['post_copied'] = True
            if time_now % time_to_see_post == 0:
                if post_idx < len(posts):
                    self.trace.add_event(event_post_seen(self.user, posts[post_idx]))
                    posts_seen[posts[post_idx]['id']] = posts[post_idx]
                    post_idx += 1
            if time_now % time_to_check_online == 0:
                self.user = vk.users.get(user_id=self.user['id'], fields='online')[0]
                if time_now != 0:
                    print("User %s offline" % self.user['id'])
                    self.trace.add_event(event_offline(self.user))
                    break
                time_now = 0

            time_now += time_to_see_post
            time.sleep(time_to_see_post)

        add_trace(self.trace)
        user_processing[self.user['id']] = False


def add_trace(trace):
    lock.acquire()
    try:
        log.add_trace(trace)
    finally:
        lock.release()


def write_log():
    lock.acquire()
    try:
        copy_log = copy.deepcopy(log)
        with open("log.xes", 'w') as filetowrite:
            filetowrite.write(str(copy_log))
            filetowrite.close()
    finally:
        lock.release()


def event_online(user):
    e = xes.Event()
    e.attributes = [
        xes.Attribute(type="string", key="concept:name", value="online"),
        xes.Attribute(type="string", key="org:resource", value=str(user['id'])),
        xes.Attribute(type="string", key="user:name", value=str(user['id'])),
        xes.Attribute(type="date", key="time:timestamp", value=datetime.datetime.now().isoformat())
    ]
    return e


def event_offline(user):
    e = xes.Event()
    e.attributes = [
        xes.Attribute(type="string", key="concept:name", value="offline"),
        xes.Attribute(type="string", key="org:resource", value=str(user['id'])),
        xes.Attribute(type="date", key="time:timestamp", value=datetime.datetime.now().isoformat())
    ]
    return e


def event_post_seen(user, post):
    e = xes.Event()
    e.attributes = [
        xes.Attribute(type="string", key="concept:name", value="post_seen"),
        xes.Attribute(type="string", key="org:resource", value=str(user['id'])),
        xes.Attribute(type="string", key="post:id", value=str(post['id'])),
        xes.Attribute(type="string", key="post:type", value=str(post['type'])),
        xes.Attribute(type="string", key="post:is_ads", value=str(post['marked_as_ads'])),
        xes.Attribute(type="string", key="group:id", value=str(post['group_id'])),
        xes.Attribute(type="date", key="time:timestamp", value=datetime.datetime.now().isoformat())
    ]
    return e


def event_post_liked(user, post):
    e = xes.Event()
    e.attributes = [
        xes.Attribute(type="string", key="concept:name", value="post_liked"),
        xes.Attribute(type="string", key="org:resource", value=str(user['id'])),
        xes.Attribute(type="string", key="post:id", value=str(post['id'])),
        xes.Attribute(type="string", key="post:type", value=str(post['type'])),
        xes.Attribute(type="string", key="post:is_ads", value=str(post['marked_as_ads'])),
        xes.Attribute(type="string", key="group:id", value=str(post['group_id'])),
        xes.Attribute(type="date", key="time:timestamp", value=datetime.datetime.now().isoformat())
    ]
    return e


def event_post_copied(user, post):
    e = xes.Event()
    e.attributes = [
        xes.Attribute(type="string", key="concept:name", value="post_copied"),
        xes.Attribute(type="string", key="org:resource", value=str(user['id'])),
        xes.Attribute(type="string", key="post:id", value=str(post['id'])),
        xes.Attribute(type="string", key="post:type", value=str(post['type'])),
        xes.Attribute(type="string", key="post:is_ads", value=str(post['marked_as_ads'])),
        xes.Attribute(type="string", key="group:id", value=str(post['group_id'])),
        xes.Attribute(type="date", key="time:timestamp", value=datetime.datetime.now().isoformat())
    ]
    return e


def load_news_feed(group_ids):
    news_feed = []
    for group_id in group_ids:
        try:
            posts = vk.wall.get(owner_id="-" + str(group_id), count=posts_count / len(group_ids))["items"]
        except:
            continue
        for post in posts:
            if "is_pinned" in post and post["is_pinned"]:
                continue
            news_feed.append({
                "id": post["id"],
                "group_id": group_id,
                "date": post["date"],
                "type": post["post_type"],
                "marked_as_ads": post["marked_as_ads"]
            })
    return sorted(news_feed, key=lambda p: p['date'], reverse=True)


def load_liked_posts(user_id, posts):
    liked_posts = {'liked': [], 'copied': []}
    for post_id in posts:
        try:
            liked_post = vk.likes.isLiked(user_id=user_id, owner_id="-" + str(posts[post_id]['group_id']),
                                          item_id=post_id,
                                          type=posts[post_id]['type'])
        except:
            continue
        if not ('post_liked' in posts[post_id] and posts[post_id]['post_liked']) and liked_post['liked']:
            liked_posts['liked'].append(posts[post_id])
        if not ('post_copied' in posts[post_id] and posts[post_id]['post_copied']) and liked_post['copied']:
            liked_posts['copied'].append(posts[post_id])
    return liked_posts


lock = threading.Lock()

log = xes.Log()
log.classifiers = [
    xes.Classifier(name="org:resource", keys="org:resource"),
    xes.Classifier(name="concept:name", keys="concept:name"),
    xes.Classifier(name="time:timestamp", keys="time:timestamp")
]

user_processing = {}
