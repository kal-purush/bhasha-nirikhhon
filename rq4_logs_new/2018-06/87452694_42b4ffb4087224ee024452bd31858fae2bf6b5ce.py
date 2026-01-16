given=list(map(int,input().split(",")))
scorecard={"player1":0,"player2":0,"player3":0,"player4":0}
currentbatsman={0:0,1:0}
iterator=0
print(given)
for i in range(len(given)):
  if(i%6==0 and i!=0):
    iterator+=1
  iterator=iterator%2
  if (given[i]%2==0):
    currentbatsman[iterator]+=given[i]
  else:
    currentbatsman[iterator]+=given[i]
    iterator+=1
print (currentbatsman[0])
print (currentbatsman[1])