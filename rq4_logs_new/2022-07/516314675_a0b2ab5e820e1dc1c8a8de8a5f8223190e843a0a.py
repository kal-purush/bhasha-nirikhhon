phone_book = {"John": "0987-654321", "Andrew": "0912-343531"}
class Phonebook:
  def __init__(self, pb):
    self.pb = pb
    self.val_pb = pb.values() # values of pb

  def add(self, name, phone):
    self.pb["{}".format(name)] = phone # add the new dict into pb
    return self.pb

  def find(self, key):
    if key in self.pb: # key in pb.keys
      print("{}'s phone is {}".format(key, self.pb[key])) 

    elif key in self.val_pb: # key in pb.values
      pb_ = {v : k for k, v in self.pb.items()} # change keys and values position
      print("{}'s phone is {}".format(pb_[key], key))
      
    else: # if type wrong key
      print("{} is not in phone book!".format(key))

  def print_all(self):
    for i,j in self.pb.items():
      print(i, "\t/", j)
pb = Phonebook(phone_book)
pb.find("0987-654321")

