import vk_api
import model

# https://oauth.vk.com/authorize?client_id=7230301&redirect_uri=https://localhost&response_type=code&scope=friends groups offline
# https://oauth.vk.com/access_token?client_id=7230301&client_secret=EfoAqkm3YcsIaRlRd7cq&code=683467eade23dd4136&redirect_uri=https://localhost
vk_session = vk_api.VkApi(token="d0bc8ad7183848802f0716cc9113284b6fa5fd2c642898dbf71234ab8947ea560aaa6b6da23ed785aca91")
vk = vk_session.get_api()

user = vk.friends.get(user_id=361950485)#, fields='bdate, photo_100, city, country, home_town, sex, games, online, domain, has_mobile, contacts, site, education, universities, schools, status, last_seen, followers_count, common_count, occupation, nickname, relatives, relation, personal, connections, exports, activities, interests, music, movies, tv, books, games, about, quotes, can_post, can_see_all_posts, can_see_audio')
print(user)
#print(vk.groups.getById(group_id=20629724, fields='activity'))