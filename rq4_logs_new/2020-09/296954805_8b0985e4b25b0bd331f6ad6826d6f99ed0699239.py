import voice_o, pyttsx3, numpy as np, matplotlib.pyplot as plt

engine = pyttsx3.init()
engine.setProperty('rate', 120)
engine.setProperty('volume', 0.9)


print('Hello Master Sam. I am your voice assistant Alfred.')
engine.say("Hello Master Sam. I am your voice assistant Alfred.")
print("How can I help you Sir?")
engine.say(" How can I help you Sir?")
engine.runAndWait()

#p = pq.Plot2D()
#p.animation()

v = voice_o.voice(chunk = 4410 * 5)
dn = 0
dp = 0
count = 0
p = True

while p == True:
    d = v.trace_sp()
    dn = d.size
    print(dn)
    q = dp == dn
    if q == False & count < 3:
        print('The task has been done Sir. Any new task Sir?')
        engine.say("The task has been done Sir. Any new task Sir?")
        engine.runAndWait()
        dp = dn
        count = 0
    elif (q == True) & (count < 3):
        print('Any new task Sir?')
        engine.say("Any new task Sir?")
        engine.runAndWait()
        dp = dn
        count += 1
    elif count >= 2:
        print('Entering to the sleep mode. Have a nice day master Sam.')
        engine.say("Entering to the sleep mode. Have a nice day master Sam.")
        engine.runAndWait()
        p = False