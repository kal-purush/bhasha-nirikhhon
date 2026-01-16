"""Given a dictionary and a value, give list of all keys which have that value. In case of nesting, the keys should be separated by a dot.
   {'a' : 'apple',
     'b' : 'bobb',
     'c' : {
         'd' : 'dog'
       },
      'e' : 'dog'
     }
  I/P -> 'dog'
  O/P -> ['e', 'c.d'] """

d = {'a' : 'apple','b' : 'bobb','c' : {'d' : {'f':'dog'}},'e' : {'g':'dog'}}


def value_search(value: int, dictionary: dict, matches=[], temp=""):
    for key in dictionary:
        if type(dictionary[key]) is dict:
            temp += key + "."
            value_search(value, dictionary[key], matches, temp)
        if value == dictionary[key]:
            temp += key
            matches.append(temp)
        temp = ""
    return matches


print(value_search('dog', d))