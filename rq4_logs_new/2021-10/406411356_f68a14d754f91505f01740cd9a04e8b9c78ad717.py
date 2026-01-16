# Организуйте архитектуру приложения “База данных” (псевдо). В роли базы данных у вас 
# будет класс  Database, который будет хранить данные в виде переменной списка.


# Class Database. 
class TestDatabase: 

    def __init__(self):
        self.db = ['A', 'B', 'E', 'S', 'A', 'Q']
 
        
# Interprinator input information from user       
class _Interpritator:
    _database = TestDatabase()

    def __init__(self):
        pass

    def _choise(self, user_input):
        if user_input == "s":
            print("You choise search information '{}'".format(user_input))
            _search_info = input("What information you search? - ")
            _searching = _SearchInfo()
            _result = _searching._search(self._database, _search_info)
            return _result
        elif user_input == "i":
            print("You choise input information '{}'".format(user_input))
            _input_info = input("What information you added? - ")
            _inputing = _InputInfo()
            self._database.db, _result = _inputing._set_info(self._database, _input_info)
            return _result
        else:
            print("You choise output information '{}'".format(user_input))
            _output_info = _OutputInfo()
            _result = _output_info._get_database_information(self._database)
            return _result


# Search information class
class _SearchInfo():
    def _search(self, _database, search_info):
        try:
           index_info = _database.db.index(search_info)
           if index_info > -1:
                return "The information you are looking for is in this database under a serial number {}".format(index_info)
        except ValueError:
            return "Sorry! The information you are looking for dont have this database"          


# Input information class
class _InputInfo:
    def _set_info(self, _database, _input_info):
        _database.db.append(_input_info)
        return _database.db, "Your information add in the 'TestDatabase' from number {}".format(_database.db.__len__() - 1)


# Output information class
class _OutputInfo:
    _result = ''
    _counter = 0
    def _get_database_information(self, _database):
        for element in _database.db:
            self._result += ("element # {}\tValue: {}{}".format(self._counter, element, '\n'))
            self._counter += 1
        return self._result


# User interface class 
class UserInterface:
    def __init__(self):
        pass

    def choise_of_action(self):
        while True:
            print("What you have action with this 'Test Database':\n\t", 
                    "s - search  information\n\t", 
                    "i - input information\n\t", 
                    "o - output information\n\t",
                    "q - quit for this program\n",
                    "What is you choise? - ")
            user_input = input()
            if user_input == "s" or user_input == "i" or user_input == "o":
                _interpritator = _Interpritator()
                _result = _interpritator._choise(user_input)
                print(_result)
            elif user_input == "q":
                print("You choise '{}' - Quit\nProgramm 'Test Database' is closed! Good by".format(user_input))
                break
            else:
                print("You input wrong char '{}'. Try again".format(user_input))


class main:
    user_interface = UserInterface()
    user_interface.choise_of_action()

if __name__ == '__main__':
    main()
    