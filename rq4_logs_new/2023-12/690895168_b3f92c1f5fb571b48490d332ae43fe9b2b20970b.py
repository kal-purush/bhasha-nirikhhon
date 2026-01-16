import random
import string
def random_password(k):
    """
    pass the length of the value to module
    
    create a random password with required length generator with 1 capital,1 numerical
    1 special charcter and remaning all small charcters 
        
    it required only to pass required length only
        
    """
    upper=string.ascii_uppercase
    lower=string.ascii_lowercase
    digits=string.digits
    special_charcter="@_-"
    password="".join(random.choice(upper)+random.choice(digits)+random.choice(special_charcter))
    password+="".join(random.choices(lower,k=k-3))
    password=list(password)
    random.shuffle(password)
    return "".join(password)
