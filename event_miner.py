#!/usr/local/bin/python3

import threading
import time
import xes
import threading
from datetime import datetime, timezone
import copy
import os

# https://oauth.vk.com/authorize?client_id=7230301&redirect_uri=https://localhost&response_type=code&scope=friends groups offline
# https://oauth.vk.com/access_token?client_id=7230301&client_secret=EfoAqkm3YcsIaRlRd7cq&code=683467eade23dd4136&redirect_uri=https://localhost

posts_count = 3
time_tick = 15
time_to_see_post = 15
time_to_check_likes = 1 * 60
time_to_check_online = 1 * 60  # 1 min
time_to_update_news = 3 * 60  # 5 min
time_to_update_user_news = 3 * 60  # 3 min
time_to_update_group_trace = 3 * 60  # 3 min
time_to_write_log = 3 * 60  # 3 min


class GroupEventMiner(threading.Thread):
    def __init__(self, vk, news_feed, ontology, event_log, community):
        threading.Thread.__init__(self)
        self.community = community
        self.vk = vk
        self.news_feed = news_feed
        self.ontology = ontology
        self.event_log = event_log
        self.owner_id = "-" + str(self.community.g_id)
        self.trace = xes.Trace()

    def run(self):
        print("start mining events for community %s" % self.community.g_id)

        time_now = 0
        post_idx = 0
        posts = []
        posts_seen = {}

        while True:
            # Load or update news from community
            if time_now % time_to_update_news == 0:
                self.load_news_feed()

            # Update group event trace
            if time_now % time_to_update_group_trace == 0:
                self.event_log.add_trace(self.trace)
                self.trace = xes.Trace()

                time_now = 0

            # time tick
            time_now += time_tick
            time.sleep(time_tick)

    def load_news_feed(self):
        posts = self.vk.get_posts(self.owner_id, posts_count, self.news_feed.get_last_post_id(self.owner_id))

        for post in posts:
            self.ontology.save_post(post)
            if post.is_reposted():
                self.trace.add_event(self.event_log.event_post_copied(self.owner_id, post))
            else:
                self.trace.add_event(self.event_log.event_post_add(self.owner_id, post))

        self.news_feed.add_posts(self.owner_id, posts)


class UserEventMiner(threading.Thread):
    def __init__(self, vk, news_feed, ontology, event_log, person):
        threading.Thread.__init__(self)
        self.vk = vk
        self.news_feed = news_feed
        self.event_log = event_log
        self.ontology = ontology
        self.person = person
        self.owner_id = person.u_id
        self.user_online = False
        self.trace = None

    def run(self):
        print("start mining events for user %s" % self.person.u_id)

        time_now = 0
        post_idx = 0
        posts = []

        while True:
            # Check user likes and reposts
            if self.user_online and time_now % time_to_check_likes == 0:
                liked_posts = self.vk.get_liked_posts(self.person)

                for liked_post in liked_posts:
                    self.trace.add_event(self.event_log.event_post_liked(self.person.u_id, liked_post))
                    self.person.post_liked.append(liked_post)
                    self.ontology.post_liked(self.person.u_id, liked_post)

            # Check user status
            if time_now % time_to_check_online == 0:
                is_online = self.vk.is_user_online(self.person)
                if not self.user_online and is_online:
                    print("user %s online" % self.person.u_id)
                    self.trace = xes.Trace()
                    self.trace.add_event(self.event_log.event_online(self.person.u_id))
                    self.user_online = True
                elif self.user_online and not is_online:
                    print("user %s offline" % self.person.u_id)
                    self.trace.add_event(self.event_log.event_offline(self.person.u_id))
                    self.event_log.add_trace(self.trace)
                    self.user_online = False
                    time_now = 0

            # Load user news feed
            if self.user_online and time_now % time_to_update_user_news == 0:
                time.sleep(10)
                posts = self.news_feed.get_user_news(self.person)
                post_idx = 0

            # Check user see new post
            if self.user_online and time_now % (time_to_see_post * (len(self.person.groups) // 30 + 1)) == 0:
                if post_idx < len(posts):
                    self.trace.add_event(self.event_log.event_post_seen(self.person.u_id, posts[post_idx]))
                    self.person.posts_seen.append(posts[post_idx])
                    self.ontology.post_viewed(self.person.u_id, posts[post_idx])
                    post_idx += 1

            # Load or update news from person
            if time_now % time_to_update_news == 0:
                self.load_news_feed()

            # time tick
            time_now += time_tick
            time.sleep(time_tick)

    def load_news_feed(self):
        posts = self.vk.get_posts(self.owner_id, posts_count, self.news_feed.get_last_post_id(self.owner_id))

        for post in posts:
            self.ontology.save_post(post)
            if post.is_reposted():
                self.trace.add_event(self.event_log.event_post_copied(self.owner_id, post))
            else:
                self.trace.add_event(self.event_log.event_post_add(self.owner_id, post))

        self.news_feed.add_posts(self.owner_id, posts)
