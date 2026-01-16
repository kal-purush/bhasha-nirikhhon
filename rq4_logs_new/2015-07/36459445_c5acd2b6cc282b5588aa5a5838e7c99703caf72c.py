#------------------------------------------------------------
# Challenge #223: "Eel Of Fortune"
# Difficulty: Intermediate
# July 15, 2015
# Brandon Newbright
#------------------------------------------------------------

def problem(word, slur):
  return slur == "".join(c for c in word if c in slur)

words = ["synchronized", "misfunctioned", "mispronounced", "shotgunned", "snond"]

for w in words:
  print(problem(w, "snond"))