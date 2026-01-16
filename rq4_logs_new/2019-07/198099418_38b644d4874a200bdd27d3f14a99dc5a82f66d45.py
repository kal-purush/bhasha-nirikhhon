class Mapping():

    def __init__(self):
        '''This method initializes the mapping class'''
        self.my_dict = {}

    def create_dict(self):
        '''This method creates a dictionary that takes four Names together with respective ages '''
        for i in range(4):
            self.name = input('Please enter a name:')
            self.age = int(input('Please enter age:'))
            self.my_dict[self.name] = self.age
        return self.my_dict 

    def get_item(self):
        '''This uses the key to search for a value in the dictionary'''
        self.name = input('Please enter a name:')
        if self.name in self.my_dict:

            return self.my_dict[self.name]
        else:
            print('Item is not in the dictionary')

    def clear_dict(self):
        '''This removes all items in the dictionary'''
        self.my_dict.clear()
    def update(self):
        '''This method adds element(s) to the dictionary if the key is not in the dictionary.

        If the key is in the dictionary, it updates the key with the new value.'''
        self.name = input('Please enter a name:')
        self.age = int(input('Please enter age:'))
        self.my_dict[self.name] = self.age
        return self.my_dict
    def dict_len(self):
        '''This method returns the length of the dictionary'''
        return len(self.my_dict)

    def dict_keys(self):
        '''This returns the names(keys) in the dictionary'''
        return self.my_dict.keys()

    def dict_values(self):
        '''This returns dictionary ages(values)'''
        return self.my_dict.values()

    def pop_item(self):
        '''This method removes an observation at a specific index and returns it'''
        self.name = input('Please enter a name:')
        self.my_dict.pop(self.name)
        return self.my_dict

    def remove_item(self):
        '''This permanently removes an item from the dictionary'''
        self.name = input('Please enter a name in the dictionary:')
        if self.name in self.my_dict:
            del self.my_dict[self.name]
        else:
            print('The name is not in the dictionary')
        return self.my_dict