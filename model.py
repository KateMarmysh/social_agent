class Person:
    friends = []
    groups = []
    interests = []

    def __init__(self, u_id, first_name, last_name, photo, domain, gender, birth_day):
        self.u_id = u_id
        self.first_name = first_name
        self.last_name = last_name
        self.gender = gender
        self.birth_day = birth_day
        self.photo = photo
        self.domain = domain


class Community:
    def __init__(self, g_id, name, activity):
        self.g_id = g_id
        self.name = name
        self.activity = activity
