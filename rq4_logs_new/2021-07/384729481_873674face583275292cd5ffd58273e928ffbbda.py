import osmapi
import re
import sys

def main():
  api = osmapi.OsmApi()

  regex = re.compile(r"(\d+)")

  users = {}
  creators = {}

  with open("refs.txt") as fp:
      Lines = fp.readlines()
      for l in Lines:
        m = regex.search(l)
        if m:
          r = m.group(0)
          rhist = api.RelationHistory(r)
          lowest = sys.maxsize
          creator = None

          for k, v in rhist.items():
            user = v["user"]
            version = v["version"]
            
            if version < lowest:
              lowest = version
              creator = user

            if user in users:
              users[user] = users[user] + 1
            else:
              users[user] = 1


          if not creator:
            print("Relation %d does not have a creator? Wat?" % r)
          else:
            if creator in creators:
              creators[creator] = creators[creator] + 1
            else:
              creators[creator] = 1

  def print_numdict_reverse(d, topn=None, minval=1):
    for i,k in enumerate(sorted(d, key=d.get, reverse=True)):
      if (not topn or i < topn) and (d[k] >= minval):
        print("%s: %d" % (k, d[k]))

  topn = 20
  #  minval = 3

  print("Users:")
  print_numdict_reverse(users)

  print("Users (top %d):" % topn)
  print_numdict_reverse(users, topn)

  #  print("Users (min edits: %d):" % minval)
  #  print_numdict_reverse(users, minval = minval)


  topn = 20
  #  minval = 2

  print("Creators:")
  print_numdict_reverse(creators)

  print("Creators (top %d):" % topn)
  print_numdict_reverse(creators, topn)

  #  print("Creators (min creates: %d):" % minval)
  #  print_numdict_reverse(creators, minval = minval)

if __name__ == '__main__':
    main()