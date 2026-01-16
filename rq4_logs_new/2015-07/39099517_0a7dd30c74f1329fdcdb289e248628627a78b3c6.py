#!/usr/bin/python

import time, sys, os, commands, datetime, re
import xmpp
import pyDaemon

from jimbo_config import *

LOGFILE = '/var/log/jimbo.log'
PIDFILE = '/var/run/jimbo.pid'
FIVE_MINUTES =  datetime.timedelta(0, 0, 0, 0, 5)
METHOD_CHARS_RE = re.compile('[A-Z|a-z|_]')

class Jimbo(xmpp.Client):
    
    def __init__(self, host, proxy, username, password, resource):
        
        self.Namespace = 'jabber:client'
        self.DBG = 'client'
        
        xmpp.Client.__init__(self, HOST)
        
        self.connect_and_auth()
        
        self.connected = True
        
    def connect_and_auth(self):
        
        if PROXY:
            self.connect( server=PROXY )
        else:
            self.connect()

        if not self.isConnected():
            print "ERROR - no server"
            sys.exit(-1)
        
        self.auth(USERNAME, PASSWORD, RESOURCE)
        self.sendInitPresence()        
        
        self.monitor_last_run = None
        
        roster = self.getRoster()
        contact_roster_entry = roster.getItem(CONTACT)
        if not contact_roster_entry:
            print "SUBSCRIBING TO: "+ CONTACT
            roster.Subscribe(CONTACT)    
    
        for contact in AUTHORIZED_CONTACTS:
            subscription = roster.getSubscription(CONTACT)
            if subscription != 'both' and subscription != 'from':
                print "AUTHORIZING: "+ CONTACT
                roster.Authorize(CONTACT)
    
        self.RegisterHandler('message', self.onMessageReceived)

    def cmd_uptime(self, sender, args):
        'Server uptime'
        self.send_message(sender, self.uptime())
    
    def cmd_load(self, sender, args):
        'Load average'
    
        load_average = ' '.join( [ str(x) for x in os.getloadavg() ] )
        self.send_message(sender, load_average) 
    
    def is_time_to_monitor(self):

        now = datetime.datetime.now()
    
        if not self.monitor_last_run or now > self.monitor_last_run + FIVE_MINUTES:
           self.monitor_last_run = now
           return True
    
        return False 
    
    def cmd_monitor(self, sender, args):
        'Check everything is ok'
    
        has_override = (not args or not "cron" in args)
    
        if not self.is_time_to_monitor() and not has_override:
            return
    
        self.check_processes(sender, has_override)
        self.check_load(sender)
    
    def check_processes(self, sender, has_override):
    
        ps_output = commands.getoutput("ps -e | awk '{print $4}'")
        running = ps_output.split('\n')
    
        check_result = ['Process status:']
        important_processes = ['apache2', 'mysqld', 'memcached', 'exim']
        has_error = False
        for process_name in important_processes:
            if process_name in running:
                check_result.append( process_name +' OK' )
            else:
                check_result.append( process_name +' NOT FOUND' )
                has_error = True
    
        if has_override or has_error:
            self.send_message(sender, '\n'.join(check_result))
    
    def check_load(self, sender):
    
        tenMinuteAverage = os.getloadavg()[1]
        if tenMinuteAverage > 3:
            self.send_message(sender, "ALERT: Load averge is high - type 'load' for details")
    
    #
    # JABBER / Framework stuff
    #
    
    def cmd_help(self, sender, args):
        'Display help message'

        help = ["Commands:\n"]
        command_methods = [ eval('self.'+ x) for x in dir(self) if x.startswith('cmd_') ]
        for cmd in command_methods:
            help.append( cmd.__name__.split('_')[1] +": "+ str(cmd.__doc__) )
      
        self.send_message(sender, '\n'.join(help))
    
    def onMessageReceived(self, connection, mess):
    
        sender = mess.getFrom() # sended is a xmpp.protocol.JID
        text = mess.getBody()
    
        if not sender.getStripped() in AUTHORIZED_CONTACTS:
            print "ERROR: User "+ str(sender.getStripped()) +" not authorized"
            self.send_message(sender, "You are not authorized")
            return
    
        elements = text.split(' ')
        (cmd, args) = elements[0], elements[1:] 
    
        if not METHOD_CHARS_RE.match(cmd):
            self.send_message(sender, "Invalid characters received: '"+ cmd +"'")
            return
        
        try:
            method_to_call = eval('self.cmd_'+cmd)
            method_to_call(sender, args)
        except AttributeError, e:
            print e 
            print "ERROR: '"+ cmd +"' not recognized - try 'help'"
            self.send_message(sender,"'"+ cmd +"' not recognized")
    
    def send_message(self, to, str_message):
        self.send( xmpp.Message(to, str_message) )

    # uptime method comes from Dave Smith's blog
    # http://thesmithfam.org/blog/2005/11/19/python-uptime-script/
    def uptime(self):
    
         try:
             f = open( "/proc/uptime" )
             contents = f.read().split()
             f.close()
         except:
            return "Cannot open uptime file: /proc/uptime"
    
         total_seconds = float(contents[0])
    
         # Helper vars:
         MINUTE  = 60
         HOUR    = MINUTE * 60
         DAY     = HOUR * 24
    
         # Get the days, hours, etc:
         days    = int( total_seconds / DAY )
         hours   = int( ( total_seconds % DAY ) / HOUR )
         minutes = int( ( total_seconds % HOUR ) / MINUTE )
         seconds = int( total_seconds % MINUTE )
    
         # Build up the pretty string (like this: "N days, N hours, N minutes, N seconds")
         string = ""
         if days> 0:
             string += str(days) + " " + (days == 1 and "day" or "days" ) + ", "
         if len(string)> 0 or hours> 0:
             string += str(hours) + " " + (hours == 1 and "hour" or "hours" ) + ", "
         if len(string)> 0 or minutes> 0:
             string += str(minutes) + " " + (minutes == 1 and "minute" or "minutes" ) + ", "
         string += str(seconds) + " " + (seconds == 1 and "second" or "seconds" )
    
         return string


#
# End Jimbo class
#

#
# MAIN
#

def run():

    while 1:

        try:
            conn = Jimbo(HOST, PROXY, USERNAME, PASSWORD, RESOURCE)

            while conn.isConnected():
                conn.Process(1)
                conn.cmd_monitor(CONTACT, ["cron"])
                time.sleep(2)
        except Exception, e:
            print "Caught exception: ", e

        print "Disconnected - pausing then attempting reconnection"
        time.sleep(5)

class Log:
    """file like for writes with auto flush after each write
    to ensure that everything is logged, even during an
    unexpected exit."""
    def __init__(self, f):
        self.f = f
    def write(self, s):
        self.f.write(s)
        self.f.flush()

if __name__ == "__main__":

    pyDaemon.createDaemon()
    sys.stdout = sys.stderr = Log(open(LOGFILE, 'a+'))
    run()