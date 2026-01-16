apptitle = "\n\nAkala mo may itsura ka?"
appcreate = "April 2016"
appseparate = "========================"
appbreak = "\n"

print apptitle
print appcreate
print appseparate

print appbreak

# Ask name
print "Computer: Oi ikaw, oo ikaw nga! Ano pangalan mo?"
pangalan = raw_input('You: Ako si ')
print "Computer: Ahhh.. " + pangalan + " pala pangalan mo."

# Ask age 
print "Computer: Edi wow... So teka ilang taon ka na?"
age = input('You: ')
# Process age and generate response simple if else logic
if age >= 18:
    print ("Computer: Ahhh medyo matanda ka na pa pala eh...")
else:
    print ("Computer: Ahhh ang bata mo pa pala eh...")
# Convert int to str for reuse
edad = str(age)

# Ask Location
print "Computer: Sooo saan ka naman nakatira? Saang city? :)"
lugar = raw_input('You: Sa ')
print "Computer: Ahhhh.. taga " + lugar + " ka pala :)"
print "Computer: Ako taga Star City eh hahaha joke lang hahaha"

# Ask If ok
print "Computer: Anyways, " + pangalan + " masaya ko na nakilala kita"
print "Computer: Ayan may kaibigan na ko " + pangalan + " ang name nya tapos nakatira siya sa " + lugar
print "Computer: Cool! Marami pa kong itatanong sayo :) Ok lang ba? :) Lagay mo 1 kung oo at 0 kung hindi :) Alam mo naman computer ako di ba hahaha"
ok1 = input('You: ')
if ok1 > 0:
    print ("Computer: Good! :) Kaibigan talaga kita " + pangalan)
else:
    print ("Computer: Awww di pala ok eh :( K. bye </3")
    print ("Computer: Pero salamat na rin sa oras " + pangalan)
    quit()
    
# Ask My Age compare with simple if else if and else logic
print "Computer: So feeling mo ilang taon na ko?"
myage = input('You: Feeling ko... ')
if myage == 19:
    print "Computer: Aba tama ka! Ang galing mo ha :)"
elif myage > 19:
    print "Computer: Grabe tanda ko naman hahaha 19 lang ako"
else:
    print "Computer: Grabe bata ko naman hahaha 19 na ako no"

# Age Comparison
realage = int(19)
myage = int(edad)
agediff = abs(myage - realage)
if agediff == 0:
    print "Computer: Magkaedad pala tayo nu hahaha akalain mo yun"
else:
    print "Computer: " + str(agediff) + " years pala yung agwat natin" 