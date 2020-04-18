#!/usr/local/bin/python3

import vk_api
from datetime import datetime, timezone

# 44239068, 45388386, 361950485, 235995129, 132549939, 135282929, 80817183, 90271290, 166286700]
# https://oauth.vk.com/authorize?client_id=7230301&redirect_uri=https://localhost&response_type=code&scope=friends groups offline
# https://oauth.vk.com/access_token?client_id=7230301&client_secret=EfoAqkm3YcsIaRlRd7cq&code=683467eade23dd4136&redirect_uri=https://localhost
vk_session = vk_api.VkApi(token="b2159813ff701b26058f147c281de1bf871515be0428e80add2c2930290802050f7778cf7ae1c3644e6f8")
vk = vk_session.get_api()

groups = [186468555, 3551694, 59218056, 60397113, 151288610,
          110581128, 40202469, 30210603, 46038605]

result = 15 * (31 // 30 + 1)
print(result)
