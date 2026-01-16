

import smtplib
import mailbox
import time
import os, pwd

class Elliot :




    def __init__(self, guideName, traineeName) :

        self.guideName   = guideName;
        self.traineeName = traineeName;

        #objectives = [ "passwd", "ls", "cdAndNano", "cpAndRm", "psAndTop" ] 
        #objectives = [ "psAndTop" ] 
        objectives = [ "apt", "done" ] 


        for objectiveName in objectives :

            print "Starting objective "+objectiveName

            package = __import__("objectives."+objectiveName, fromlist=[''])

            self.makeObjective(package)




    def makeObjective(self, package) :

        # Init
        print "> Initializing"
        objective = package.objective(self)

        # Send mail
        print "> Sending mail"
        self.sendMail(self.guideName, 
                self.traineeName, 
                objective.title(), 
                objective.description());

        # Wait for mail reply
        print "> Waiting for mail reply"

        while True :

            time.sleep(1)

            check = self.checkMail()
            if (check == None) : continue

            print "  > Received a new email"
            
            if (check == "skip") :
                print "> Skipping objective"
                break

            if (objective.checkIfComplete() == True) :
                print "> Objective complete"
                break
            else :
                print "  > Objective not complete"
                self.sendMail(self.guideName, 
                              self.traineeName, 
                              objective.title(), 
                              objective.sorry());



    def sendMail(self, from_, to, subject, text) :

        server = "localhost"

        from_ = from_+"@"+server
        to    = to+"@"+server

        message = """
From: %s
To: %s
Subject: %s

%s

    -- %s
""" % (from_, to, subject, text, "Elliot")


        # Send the mail

        server = smtplib.SMTP(server)
        server.sendmail(from_, [ to ], message)
        server.quit()





    def checkMail(self) :

        path = "/var/mail/elliot"
        mb = mailbox.mbox(path)

        for key, message in mb.iteritems() :

            subject = message["Subject"]

            mb.lock()
            mb.remove(key)
            mb.flush()
            mb.unlock()

            # Dirty hack
            uid = pwd.getpwnam("elliot").pw_uid
            os.chown(path,uid,-1) 

            print subject

            return subject

        return None




def main() :
    print "Starting Elliot ..."
    Elliot("elliot", "futurenerd")

main()

