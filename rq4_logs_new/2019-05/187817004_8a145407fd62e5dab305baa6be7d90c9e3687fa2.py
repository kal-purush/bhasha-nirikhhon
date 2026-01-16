import time
import sys
import random
from datetime import datetime  
from datetime import timedelta


typing_speed = 150 #wpm
input_sequence = ""

def progress(t):
    stop_at = datetime.now() + timedelta(seconds=t)
    chars = ["-", "/", "|", "\\"]
    index = 0
    while datetime.now() < stop_at:
        sys.stdout.write('  ' + chars[index])
        sys.stdout.flush()
        time.sleep(0.1)
        index = index + 1 if index < 3 else 0
        sys.stdout.write('\b\b\b   \b\b\b')
        sys.stdout.flush()
        
def sleep(t):
    time.sleep(t)

def write(t):
    t = '  ' + t
    if typing_speed > 0:
        for l in t:
            sys.stdout.write(l)
            sys.stdout.flush()
            time.sleep(random.random()*10.0/typing_speed)
        print ('')
    else:
        print(t)

def input_and_save(prompt, label, indent = False):
    global input_sequence
    x = input('  ' + prompt)
    indentation_str = '   ' if indent == True else ''
    input_sequence = input_sequence + indentation_str + label + ": " + x + "\n"
    return x

def persist_input():
    global input_sequence
    currentDT = datetime.now()
    with open("feedback.txt", "a") as feedback_file:
        input_sequence = input_sequence + "Session ended at " + currentDT.strftime("%Y-%m-%d %H:%M:%S") + "\n\n\n"
        feedback_file.write(input_sequence)

sleep(3)    
write("Hi\n")
progress(2.5)
write("What is your name?\n")
currentDT = datetime.now()
name = input_and_save("User: ", "Session at " + currentDT.strftime("%Y-%m-%d %H:%M:%S"))
name = name.capitalize()
progress(2.5)
print("  Okay, ", name, "\n")

name = name + ': '
write("Artificial intelligence is taking over jobs (more specifically, tasks). But it is not a big problem because ai is making more new jobs than it has replaced. \n   For example, ai is taking 75 million jobs globally by 2022 but it is creating 133 million. \n   However, there are many people who are so worried about robots because they fear they might take their jobs. \n   In fact, they can just get into the new jobs because ai is creating or transforming into other jobs which automation isn't taking over.\n")
progress(2)
write("Do you understand what I said?\n")
progress(1)
under = input_and_save(name, "Understands Response", True)
under = under.lower()
if under == 'yes':
    write("Ok, good.\n")
elif under == 'no':
    write("Just wait; maybe you will understand in a few minutes.")
elif under == 'sure':
    write("Ok, good.\n")
else:
    write("Never mind!\n")
progress(2.5)

write("So, is artificial intelligence benefiting the humans?\n   I will begin with the factories and the customers. Artificial intelligence and robots are benefiting factories and factories would want anything that produces with more\n   efficiency.\n   And  consumers would want a better products . Robots have efficiency because every time they do exactly what they are programmed to do.\n    But for some people like bus drivers or taxi drivers, ai might be affecting them in negative ways. In addition, it might be very hard for them to  transition between jobs.\n   In conclusion, it depends, but in my opinion ai is benefiting us a lot.\n")

introMessage = "\n  Do you have any of these questions?\n"
sayQuestions = True

while True:
    if sayQuestions == True:
        progress(4)
        write(introMessage)
        write(" 1- What is AI?\n   2- When will the main job will be gone ex, doctor?\n   3- What jobs are being made by AI?\n   4- Which jobs cannot be taken away?\n   5- Will it be harder to get a job because of AI?\n   6- Which jobs are getting taken over by AI?\n   7- What is the solution?\n   8- How do governments react with AI taking over jobs? \n")
        write("Type the number of your chosen question\n")

    quest = input_and_save(name, "Questions Response", True)
    quest = quest.lower()
    sayQuestions = True
    introMessage = "\n  Do you still have any of these questions?.\n"
    
    if quest == "1":
        write("What is AI?\n  Artificial intellegence, sometimes called machine intelligence, is intelligence demonstratod by machines. Which is programmad to do something as if it is a human.")
    elif quest == "2":
        write("When will the main job will be gone ex, doctor?\n  Ai is replacing many jobs by 2022. But for doctors some say it might take until 2025. But maybe it is sooner than we think.")
    elif quest == "3":
        write("What jobs are being made by AI?\n  There are going to be many software developers so they can progam robots. However, nobody knows what jobs will AI create.")
    elif quest == "4":
        write("Which jobs cannot be taken away?\n  These jobs have 0.4% of being automated: Audiologists, Emergency Management Directors, Sales Engineers, General Dentists, Psychologists and a few more.")
    elif quest == "5":    
        write("Will it be harder to get a job because of AI?\n  No, because AI is creating more jobs than it is replacing by 58 million jobs. But you have to enter jobs that are not high risk of automation. ")
    elif quest == "6":
        write("Which jobs are getting taken over by AI?\n  Artificial intelligince is taking over repititive and simple jobs and any jobs that requires driving. \n  Ex. Truck drivers, judges, fast-food cooks, librarians, farmers, lumberjacks and many others.")
    elif quest == "7":
        write("What is the solution?\n  Today one of the most important questions you need to ask kids is, what do you want to be when you grow up? But maybe you will add to that what 5 jobs do you want when you grow up? \n  And that is true, you tell him wether the job will be automated or not.")
    elif quest == "8":
        write("How do governments react with AI taking over jobs?\n  Several cities in Holland will give citizens a basic income of about $1,000 per month. \n  But in the USA, citizens want big government help when robots and artificial intelligence take their jobs.")          
    elif quest == "no":
        write("Okay")
        progress(2.5)
        break
    else:
        write("Please choose a question from 1-8 or say No if you do not have any questions!\n")
        sayQuestions = False
        
write("Was this helpful?\n")
write("Note# This is a yes or no question.\n")
helpful = input_and_save(name, "Helpful Response", True)
progress(2.5)
helpful = helpful.lower()
if helpful == 'yes':
    pass
elif helpful == 'no':
    write("Can you say why?\n")
    nothelpful = input_and_save(name, "Why Not Helpful", True)
    progress(2.5)
write("What did you like the most in our booth.\n")
likemost = input_and_save(name, "Liked Most", True)
progress(2.5)
write("How do you think we can improve?\n")
improve = input_and_save(name, "Improvement Suggestions", True)
progress(2.5)
write("Ok, thanks.\nGoodbye.")
persist_input()
progress(10)    
 