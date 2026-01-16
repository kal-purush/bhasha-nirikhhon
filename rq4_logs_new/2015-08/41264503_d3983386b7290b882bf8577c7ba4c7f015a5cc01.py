#Upvotes -Quora Challene 1
"""
For this problem, you are given N days of upvote count data, and a fixed window size K. For each window of K days, from left to right, find the number of non-decreasing subranges within the window minus the number of non-increasing subranges within the window.
"""
debug = True

def main():
  #test variables
  items = [1,2,3,1,1]
  n = len(items)
  k = 3
  windows = n - k + 1
  if debug:
    print("Window days = " + str(windows))
  #Started off using an iterator to solve this problem, then decide that a generator would be better.

  #getNonDecreasingSubRange(items,windows)

  for x in getCountsRange(items,windows):
    print(x)


def getCountsRange(items,windows):
  #Generator method

  vote = -1
  for idx,i in enumerate(range(0,windows)):
    non_decreasing_days = 0
    non_increasing_days = 0
    if vote == -1:
        non_decreasing_days += 1
        vote = items[i]
    subList = items[i+1:i+windows ]
    for currentVote in subList:
        if vote < currentVote:
          vote = currentVote
          non_decreasing_days+= 1
        else:
          vote = currentVote
          non_increasing_days += 1


    vote =subList[0] #set the vote to the beginning of the list
    if debug == True:
      print("{3} = non-decreasing ={0}, non-increasing= {1}, output ={2}"
            .format(non_decreasing_days,non_increasing_days,non_decreasing_days - non_increasing_days,subList))

    yield non_decreasing_days - non_increasing_days
def getNonDecreasingSubRange(items, windows):
  #Use Custom iterable to return sub ranges
  #for upvoteRange in UpvoteIterable(items, windows):
  upvoteRange = UpvoteIterable(items,windows)
  vote = 0
  beginning = True
  while True:
    try:
      #Get next Tuple
      subRange = upvoteRange.next()
      #Counter variables
      non_decreasing_days = 0
      non_increasing_days = 0
      for idx,currentVote in enumerate(subRange):
        if idx == 0:
          continue
        if vote < currentVote:
          vote = currentVote
          non_decreasing_days+= 1
        else:
          vote = currentVote
          non_increasing_days += 1

      vote =subRange[0]
      if beginning == True:
        non_decreasing_days += 1
        beginning = False
      if debug:
          print("{3} = non-decreasing ={0}, non-increasing= {1}, output ={2}"
            .format(non_decreasing_days,non_increasing_days,non_decreasing_days - non_increasing_days,subRange))
      else:
        print(non_decreasing_days - non_increasing_days)
    except StopIteration:
      print("done")
      break


#Creating our own iterable for this
class UpvoteIterable(object):

    def __init__(self,values, windowDays):
        self.values = values
        self.location = 0
        self.windowDays = windowDays


    def next(self):
        if self.location >= self.windowDays:
            raise StopIteration
        #get list of values
        value = self.values[self.location:self.windowDays +self.location]

        self.location += 1
        return value

#Require to run code
if __name__=='__main__':
  main()