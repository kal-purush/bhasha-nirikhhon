class Organization:
    # all these fields are strings
    def __init__(self, address, city, state,
                 zip, missionStatement, email, phone, password, lastActivity):
        self.address = address
        self.city = city
        self.state = state
        self.zip = zip
        self.mission = missionStatement
        self.email = email
        self.phone = phone
        self.password = password
        self.activity = lastActivity

    # change the address to the new address
    def editAddress(self, address):
        self.address = address

    # change the city to the new city
    def editCity(self, city):
        self.city = city

    # add the neighboorhood field to the instance and set it to neighborhood
    def editNeighborhood(self, hood):
        self.neighborhood = hood

    # edit the state field to the new state
    def editState(self, state):
        self.state = state

    # edit the zip code or raise an error if it is ivalid
    # a valid zip code is any 5-digit string of integers
    def editZip(self, zip):
        if len(zip) != 5:
            raise ValueError('a zip code must be 5 digits long')
        elif type(zip) != int:
            raise TypeError('a zip code must be a string of integers')
        else:
            self.zip = zip

    # edit the mission statement of the organization
    def editMissionStatement(self, mission):
        self.mission = mission

    # edit the email address of the organization
    # need to check if the email is valid
    def editEmail(self, email):
        self.email = email

    # edit the phone number of the organization if it is valid
    # a phone number is a string of integers and some special characters
    def editPhoneNumber(self, phone):
        if any (c.isalpha() for c in phone):
            raise ValueError('a phone number should not contain letters')
        else:
            self.phone = phone

    # edit the password of the organization
    def editPassword(self, password):
        self.password = password

    # edit the last activity of the organization
    def editLastActivity(self, activity):
        self.activity = activity


