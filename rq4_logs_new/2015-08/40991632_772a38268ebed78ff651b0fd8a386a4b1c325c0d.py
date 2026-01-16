class load_authentication_oauth_card(NebriOS):
    listens_to = ['load_authentication_oauth_card']

    def check(self):
        return self.load_authentication_oauth_card is True

    def action(self):
        self.consumer_key = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(25))
        self.consumer_secret = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(45))
        load_card('nebrios-authentication-oauth')