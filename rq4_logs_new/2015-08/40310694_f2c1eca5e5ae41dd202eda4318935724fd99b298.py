""" -*- python -*-
    $Id: ArubaCONNECT.py,v 1.1 2002/12/09 17:30:02 wcox Exp $

"""

import ArubaXML
import httplib
import types
import base64
import os

from urllib import splittype, splithost, splitport, splituser

from ArubaErrors import ArubaConnectionError, RemoteError

class ArubaConnection:
    def __init__ (self, options):
        self.debug = 0
        self.ExecutionTrace = None
        self.DebugFlag = None
        self.TraceResults = None
        self.dtd = None
        self.AccountServer = None
        self.ApplicationName = sys.argv[0]
        self.OperatorName = os.getlogin ()
        self.CallCorrelation = None
        self.BlockSize = None
        self.MaxCount = None
        self.LogLevel = None
        if os.environ.has_key ('K_LOG_DIR'):
            self.logfilename = os.environ['K_LOG_DIR'] \
                               + '/arubaxml.dbg'
        else:
            self.logfilename = 'arubaxml.dbg'
        if options != None:
            if ((type (options) == types.DictionaryType)
                and (options.has_key ('url'))):
                # requires  http://user:passwd@host:port/path
                dummy_type, rhs = splittype (options['url'])
                host, options['path'] = splithost (rhs)
                options['user_passwd'], options['host_port'] = splituser (host)
            elif type (options) == types.StringType:
                # requires  http://user:passwd@host:port/path
                url = options
                options = {}
                dummy_type, rhs = splittype (url)
                host, options['path'] = splithost (rhs)
                options['user_passwd'], options['host_port'] = splituser (host)
            elif type (options) == types.InstanceType:
                options = options.__dict__

            for key in options.keys ():
                setattr (self, key, options[key])

        if os.environ.has_key ('ARUBA_DTD'):
            f = open (os.environ['ARUBA_DTD'], 'r')
            self.dtd = f.read()
        if os.environ.has_key ('ARUBA_LOG_LEVEL'):
            self.debug = int (os.environ['ARUBA_LOG_LEVEL'])


    def call (self, method, args):
        if self.debug & 0x01:
            self._log_message ("xml_RoundTrip...")
            self._log_message ("DEST:\n%s", str (self.__dict__))
        con = httplib.HTTP (self.host_port)
        con.set_debuglevel (0)
        con.putrequest ("POST", self.path)
        if (self.user_passwd != None):
            auth = base64.encodestring (self.user_passwd)

        msg = {
            'Request' : {
                '#ORDER' : [ 'Header', method ],  
                'Header' : {
                    '#ORDER' : [ 'AccountServer',
                                 'ApplicationName',
                                 'OperatorName',
                                 'CallCorrelation',
                                 'ExecutionTrace',
                                 'DebugFlag',
                                 'BlockSize',
                                 'MaxCount',
                                 'LogLevel',
                               ],
                },
                method              : args,
            },
        }
        if self.ApplicationName != None:
            msg['Request']['Header']['ApplicationName'] = \
                                                        self.ApplicationName
        if self.OperatorName != None:
            msg['Request']['Header']['OperatorName'] = self.OperatorName
        if self.CallCorrelation != None:
            msg['Request']['Header']['CallCorrelation'] = self.CallCorrelation
        if self.AccountServer != None:
            msg['Request']['Header']['AccountServer'] = self.AccountServer
        else:
            msg['Request']['Header']['AccountServer'] = -1
        if self.ExecutionTrace != None:
            msg['Request']['Header']['ExecutionTrace'] = self.ExecutionTrace
        if self.DebugFlag != None:
            msg['Request']['Header']['DebugFlag'] = self.DebugFlag
        if self.BlockSize != None:
            msg['Request']['Header']['BlockSize'] = self.BlockSize
        if self.MaxCount != None:
            msg['Request']['Header']['MaxCount'] = self.MaxCount
        if self.LogLevel != None:
            msg['Request']['Header']['LogLevel'] = self.LogLevel
        dtd = self.dtd
        self.TraceResults = None
        xml_msg = ArubaXML.dumps (msg)

        if self.debug & 0x01:
            self._log_message ("SEND:\n%s", str (xml_msg))

        if (self.user_passwd != None):
            con.putheader ("Authorization", "Basic %s" % auth[0:len (auth) - 1])
        con.putheader ("Content-length", "%d" % len (xml_msg))
        con.putheader ("Connection", "close")
        con.putheader ("Host", "%s" % self.host_port)
        con.endheaders ()

        con.send (xml_msg)
        
        if self.debug & 0x01:
            self._log_message ("Waiting on HTTP server...")

        errcode, errmsg, headers = con.getreply ()
        if errcode == 200:
            file = con.getfile ()
            replyStr = file.read ()
            if self.debug & 0x01:
                self._log_message ("RECV XML:\n%s", replyStr)
    
        elif errcode == 500:
            if self.debug & 0x01:
                self._log_message ("TIMEOUT: %d -- %s", errcode, errmsg)
            raise ArubaConnectionError, "Transport time out: 500 " + errmsg

        else:
            if self.debug & 0x01:
                self._log_message ("HTTP ERROR: %d -- %s", errcode, errmsg)
                self._log_message ("HEADERS:\n%s", headers)
            raise ArubaConnectionError, "Transport Failed: " + str(errcode) + " " + errmsg
    
        if replyStr[0:5] != "<?xml":
            if self.debug & 0x01:  
                self._log_message ("Response from ARUBA not in XML format")
            raise ArubaConnectionError, "Bad result data format"

        d = ArubaXML.loads (replyStr, dtd)
        if self.debug & 0x02:
            self._log_message ("AS DICT:\n%s", str (d))
        if (not d.has_key ('Request')):
            raise ArubaConnectionError, "XML result missing 'Request' element"
        if (d['Request'].has_key ('Header')):
            header = d['Request']['Header']
            del d['Request']['Header']
            if header.has_key ('AccountServer'):
                self.AccountServer = header['AccountServer']
            if header.has_key ('CallCorrelation'):
                self.CallCorrelation = header['CallCorrelation']
            if header.has_key ('TraceResults'):
                self.TraceResults = header['TraceResults']
        else:
            header = {}
            self.CallCorrelation = None
        bodykeys = d['Request'].keys ()
        if (len (bodykeys) != 1): 
            raise ArubaConnectionError, "XML result body has more than one element"
        bodykey = bodykeys[0]
        if bodykey == 'Exception':
            raise RemoteError ({ 'RemoteException' : d['Request'][bodykey] })
        if self.debug & 0x01:
            self._log_message ("xml_RoundTrip... DONE")
        return d['Request'][bodykey]


    def _log_message (self, format, *args):
        logFile = open (self.logfilename, "a")
        logFile.write (format % args)
        logFile.write ("\n")
        logFile.close ()

    def __getattr__ (self, attr):
        """Normally, this might get an attribute from the remote
        object.  In this case though, we only support calls to the
        remote object, so we'll cache a Method object in the __dict__
        for future use, and also return the Method object for this
        time."""
        forwarder = Method (self, attr)
        self.__dict__[attr] = forwarder
        return forwarder

class Method:
    """Represents a method on the remote object.

    Overloads the function call method to look like a function."""

    def __init__ (self, connection, method):
        """
        connection  the connection instance the
                    method will use
        method      the method to call on the remote object"""

        self._connection_ = connection
        self._method_ = method

    def __call__ (self, args):
        """This method enables calling instances as if they were
        functions. It will perform a remote procedure call on the
        server object the agent is connected to."""

        result = self._connection_.call (self._method_, args)

        return result