# Superclass for everything in the coding exercise.
# Globally useful things are here.
# @author Adam Daughterson

class AdamD(object):
    def __init__(self):
        self.logged = []

    # @brief Issue messages to STDOUT
    # @TODO: This is where I would add facility (eg WARN,ERROR,INFO), and multiple log locations (file,stdout,db,etc),
    #   or, implement a pre-existing Pythopn logger if I had more time.
    def log(self,thing):
        print thing