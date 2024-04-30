import requests
import subprocess
import json
import os

access_token = str(os.getenv("VK_API_TOKEN"))

f = open("data/groups.txt", "r")
groups = [line.strip() for line in f]
f.close()

for group in groups:    
    try:
        group_subscribers_count = requests.get("https://api.vk.com/method/groups.getMembers",
                                                params={
                                                        "access_token": access_token,
                                                        "v": "5.199",
                                                        "group_id": group,
                                                        "count": 0})
        count_subs_info = group_subscribers_count.json()
        count_subs = count_subs_info["response"]["count"]
        
        with open(f"data/group_{group}_subscribers.txt", "w") as file_users:
            print(f"Group {group}: Loading of group subscribers is started")
            for i in range(0, count_subs+1, 1000):
                try:
                    group_subscribers = requests.get("https://api.vk.com/method/groups.getMembers",
                                                    params={
                                                            "access_token": access_token,
                                                            "v": "5.199",
                                                            "group_id": group,
                                                            "count": 1000,
                                                            "fields": "is_closed",
                                                            "offset": i})
                    data = group_subscribers.json()
                    for user in data["response"]["items"]:
                        if not user["is_closed"]:
                            file_users.write((str(user["id"])) +"\n")
                except Exception as er:
                    print(er)
        
        with open(f"data/group_{group}_subscribers.txt", "rb+") as file_users:
            file_users.seek(-1, os.SEEK_END)
            file_users.truncate()
        
        print(f"Group {group}: Loading of group subscribers is completed")
        
    except Exception as er:
        print("An error occurred when loading group subscribers")
        print(f"KeyError: {er}")
        print("The program will now be terminated")
        subprocess.run(['pkill', 'python3.10'])