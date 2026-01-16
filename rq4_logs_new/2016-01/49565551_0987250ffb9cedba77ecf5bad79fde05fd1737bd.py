######################## Task CLASS ########################
class TTask :
    Count = 0
    def __init__ ( self, ID, Name, Day ) :
        self.ID = ID
        self.Name = Name
        self.Day = Day
        TTask.Count += 1
######################## getFileLength() FUNCTION ########################
def getFileLength ( file ) :
    line = file.readline ()
    lines = int ( line.rstrip ('\n') )
    return lines
######################## loadFiles() FUNCTION ########################
def loadFiles () : #loading files into lists of Task:TTask
    # opening files and creating lists
    newTasks_file = open ( 'newTasks.txt', 'r' )
    doneTasks_file = open ( 'doneTasks.txt', 'r' )
    newTasks_list = []
    doneTasks_list = []
    
    # loading the newTasks File and saving line elements into Task objects of a list
    for loop in range ( 0, getFileLength ( newTasks_file ) ) :
        line = newTasks_file.readline ()
        line = line.rstrip ('\n')
        # this is not a nice way, but works for now
        helpList = [0,0,0]
        helpList[0], helpList[1], helpList[2] = line.split ( '|', 3 )
        Task = TTask ( helpList[0], helpList[1], helpList[2] )         #ID|Name|Day
        # this ^^ can be fixed by knowing how to input more variables easier
        newTasks_list.insert (loop, Task)
    
        
        # loading the doneTasks File and saving line elements into Task objects of a list
    for loop in range ( 0, getFileLength ( newTasks_file ) ) :
        line = newTasks_file.readline ()
        line = line.rstrip ('\n')
        # this is not a nice way, but works for now
        helpList = [0,0,0]
        helpList[0], helpList[1], helpList[2] = line.split ( '|', 3 )
        Task = TTask ( helpList[0], helpList[1], helpList[2] )         #ID|Name|Day
        # this ^^ can be fixed by knowing how to input more variables easier
        newTasks_list.insert (loop, Task)
    # return filled lists with Tasks
    return newTasks_list, doneTasks_list
######################## printLists () FUNCTION ########################
def printLists () :

########################  ########################