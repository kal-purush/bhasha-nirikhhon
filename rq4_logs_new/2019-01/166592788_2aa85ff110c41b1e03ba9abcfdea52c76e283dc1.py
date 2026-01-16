class Review(object):
    def __init__(self, location_name,
                 location_tags,
                 lat,
                 lng,
                 review_id,
                 review_date,
                 user_id,
                 place_rate,
                 review_rate,
                 username):
        self.review_rate = review_rate
        self.place_rate = place_rate
        self.user_id = user_id
        self.review_date = review_date
        self.review_id = review_id
        self.lng = lng
        self.lat = lat
        self.location_tags = location_tags
        self.location_name = location_name
        self.username = username

    def get_csv_line(self):
        return "{}, {}, {}, {}, {}, {}, {}, {}, {}, {}\n".format(
            self.location_name,
            self.location_tags,
            self.lat,
            self.lng,
            self.review_id,
            self.review_date,
            self.user_id,
            self.review_rate,
            self.place_rate,
            self.username
        )

    @staticmethod
    def get_csv_header():
        return "location_name, " \
               "location_tags," \
               "lat," \
               "lng," \
               "review_id," \
               "review_date," \
               "user_id," \
               "review_rate," \
               "place_rate," \
               "username\n"