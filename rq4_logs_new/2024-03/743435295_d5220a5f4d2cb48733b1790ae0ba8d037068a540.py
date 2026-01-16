class Codetree:
    def __init__(self, secretCode, meetingPoint, time):
        self.secretCode = secretCode
        self.meetingPoint = meetingPoint
        self.time = time

a, b, c = input().split()
Codetree1 = Codetree(a,b,c)

print(f"secret code : {Codetree1.secretCode}")
print(f"meeting point : {Codetree1.meetingPoint}")
print(f"time : {Codetree1.time}")