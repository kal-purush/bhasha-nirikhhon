# DaBestLibEver.py
"""This wonderful Python library does great stuff,
including translating regular strings into SHOUTED STRINGS!!!"""

def shout(stuff):
  """str -> str
  Converts the string passed to it to all uppercase.
  Why aren't my doctests passing?!
  >>> shout("Hello")
  HELLO!!!!!
  >>> shout("bye")
  BYE!!!!!
  """
  return stuff.upper + "!!!1!"

if __name__ == "__main__":
  import doctest
  doctest.testmod()