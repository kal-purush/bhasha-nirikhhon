from Tkinter import*
import random
import copy
import time

edgeList = set()

class Animation(object):
    # Override these methods when creating your own animation
    def mousePressed(self, event): pass
    def keyPressed(self, event): pass
    def timerFired(self): pass
    def init(self): pass
    def redrawAll(self):
            view(courseList)
    
    # Call app.run(width,height) to get your app started
    def run(self, width=1000, height=1000):
        # create the root and the canvas
        root = Tk()
        root.title("Schedule Bot")
        self.window=root
        self.width = width
        self.height = height
        self.canvas = Canvas(root, width=width, height=height)
        self.canvas.pack()
        # set up events
        def redrawAllWrapper():
            self.canvas.delete(ALL)
            self.redrawAll()
        def mousePressedWrapper(event):
            self.mousePressed(event)
            redrawAllWrapper()
        def keyPressedWrapper(event):
            self.keyPressed(event)
            redrawAllWrapper()
        root.bind("<Button-1>", mousePressedWrapper)
        root.bind("<Key>", keyPressedWrapper)
        # set up timerFired events
        self.timerFiredDelay = 250 # milliseconds
        def timerFiredWrapper():
            self.timerFired()
            redrawAllWrapper()
            # pause, then call timerFired again
            self.canvas.after(self.timerFiredDelay, timerFiredWrapper)
        # init and get timerFired running
        self.init()
        timerFiredWrapper()
        # and launch the app
        root.mainloop()  # This call BLOCKS (so your program waits until you close the window!)

class  course(objects):
     def init(self,  name, preReqs):
         self.x = (int(name) % 1000) + 10
         self.y = (int(name) % 10)*40 + 100  # coordinates of center
         self.courseNo = name      # course number
         self.preReqs = preReqs

     def displayCirc(self):
        create_oval(x-5,y-5,x+5,y+5,fill="Blue")
        
      
def start(andrew):
    # Andrew is giving us [(c1,[...]), (c2,[...]), ...]
    courseList = []

    for i in xrange(len(andrew)):
        courseList.append(course(andrew[i][0],andrew[i][1]))


    calcEdges(courseList)
    view(courseList)


def view(courseList):
    for courseObj in courseList:
        courseObj.displayCirc()

def calcEdges(courseList):
    for courseObj in courseList:
        prereqList = courseObj.preReqs
        for preReq in PrereqList:
            drawLine(courseObj.x,courseObj.y,preReq.x,preReq.y)

        
        
start([(15122,[15112]),(15112,[]),(15213,[15122])])
Animation.run()