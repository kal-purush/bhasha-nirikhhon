import logging

logging.basicConfig(filename = "D:\\Suchi\\code\\git\\python\\logger_file.log", level = logging.DEBUG, filemode = 'w')

logger = logging.getLogger()

logger.info("We start logging issues from here in code")

print(logger.level)

def giveareaofsquare():
    valuegivenbyuser = input("Enter size of one side of square :\n")
    oneside = int(valuegivenbyuser)
    area = oneside * oneside
    logger.debug ("Check if multiplication worked")
    print ("Area of square is ", area)
    return area
    
giveareaofsquare()
logger.debug ("check if area is returned")   

def listingallbasiclogtypes():
    logger.info("This is used to print basic information while logging")
    logger.debug("This is used to debug the section of code")
    logger.warning("To show warning to user before taking an action")
    logger.critical("To show user critical messages incase of extreme failure to progress in an application")

#If you want to try this method, then change the path in code so that it creates a log file for you to see the msg in log file
listingallbasiclogtypes()