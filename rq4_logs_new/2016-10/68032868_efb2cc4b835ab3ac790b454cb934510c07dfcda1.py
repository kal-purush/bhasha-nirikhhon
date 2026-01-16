from DataMonitor import DataMon
from Sender import Sender
from daemon import Daemon

class DSystem (Daemon):
        __dsender = None
        __ddataMonitor = None

        def run(self):
            print ("Launching Sender and DataMon services")
            self.__dsender = Sender ('/tmp/dsender.pid')
            self.__ddataMonitor = DataMon ('/tmp/ddmon.pid')

        def stopAll(self):
            self.__dsender.stop()
            self.__ddataMonitor.stop()

def main ():
    sdaemon = DSystem('/tmp/dsys.pid')
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            sdaemon.start()
        elif 'stop' == sys.argv[1]:
            sdaemon.stopAll()
            sdaemon.stop()
        elif 'restart' == sys.argv[1]:
            sdaemon.restart()
        else:
            print "Unknown command"
            sys.exit(2)
        sys.exit(0)
    else:
        print "usage: %s start|stop|restart" % sys.argv[0]
        sys.exit(2)
if __name__ == "__main__":
    main()