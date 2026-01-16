class User:
    def __init__(self, id, name, website, description, email, gender, phone_number, username):
        self.id = id
        self.name = name
        self.website = website
        self.description = description
        self.email = email
        self.gender = gender
        self.phone_number = phone_number
        self.username = username
  
# Call a method Here    
if __name__ == '__main__':
    user = User(24, "Jon Doe", "http://mywebsite.com", "I am an actor", "example@example.com", "M", "+12345678", "johndoe")
    print('User ID :' ,user.id,'\n User Email :', user.email, '\n Gender: ',user.gender,'\n User Name:', user.username)