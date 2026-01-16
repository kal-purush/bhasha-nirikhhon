#encoding tecnique
chara=raw_input("enter a sentence")
print(type(chara))
thisdic={
  "a":"apple",
  "b":"bag",
  "c":"carrot",
  "d":"dodge",
  "e":"eat",
  "f":"fat",
  "g":"garbage",
  "h":"hot",
  "i":"ice",
  "j":"jackfruit",
  "k":"k",
  "l":"liquid",
  "m":"man",
  "n":"narcotics",
  "o":"or",
  "p":"paste",
  "q":"quater",
  "r":"rough",
  "s":"sharp",
  "t":"tastes",
  "u":"you",
  "v":"we",
  "x":"no",
  "y":"why",
  "z":"zzzz",
  " ":" "
}
elist=[]

nlist=[]

pos=0

while pos!=len(chara):
    gg=chara[pos]
   # elist.insert[pos,gg]
    pos=pos+1

p=0
while p!=len(elist):
    nlist[p]=thisdic.get(elist[p])
    p=p+1
for x in nlist:
    print(x)

