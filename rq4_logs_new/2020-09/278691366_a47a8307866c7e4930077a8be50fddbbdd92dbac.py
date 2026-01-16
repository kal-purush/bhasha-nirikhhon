import sys
import socket
import selectors
import types
import time

'''
ASSUMPTIONS
-DONE: the wifiCommunicator will only be initialized once for each power on of the pi0. Thus, the lightModuleClients should
continue to be stored even if the light gets disconnected from the base station and reconnected, although the corresponding
port numbers will change. (In the case of being disconnected from the base station, I should consult with the team as to 
what happens) 

-MAINLY DONE but main loop must also be changed: I am adding a variable to store the current actual state of the light module in each lightModuleClient. 
When initializing the wifiCommunicator class, I will add parameters for the main loop program to fill in this 
actual state variable with the actual current state of the light.
The wifiRecommendedStateVariable will be initialized to an unknown state until the pi0 connects to a basestation and attempts
to change the said state.

-DONE: MAINLY DONE but needs to be cleaned up, especially in the wificommunicator functions: It is the job of the multiconnclientclass program to deal with confirming for the basestation that the light has
actually changed state (ie name, brightness, etc.). Ie process as follows: state change request received; wifiRecommendedStateVariable
changes (perhaps we should also change the actual current state variable to unknown); then the confirm state is called by the main program
and the actual current state variable is updated with that; then whenever the basestation wants to confirm a change it calls with a confirm 
command over the wifi, where the pi0 checks if the actual and recommended variables match yet

-DONE: TO BE DONE ie with the try catch: I have to have a plan for what to do when the piui disconnects from the base station. In this case
I think the base station will continue to hold all the information about all the lights. I should ask the
rest of the team whether we should inform the light modules that the piui has been disconnected from them.
In theory it is possible to go without informing the light modules whne the piui gets disconnected and instead
deal with this issue only in the basestation, as there are no commands that the light modules have to perform
which require confirmation from the piui.
If the piui gets disconnected, I can assume that the wifiRecommendedStateVariable will remain what it was before the piui
got disconnected.
If the pi0 is disconnected from the base station, I will set wifiRecommendedStateVariables to None.

-DONE: Start wifiState in the same state as the current actualState

-LATER: I WILL ASLO HAVE TO ADD A TIMER COMPLETE OFF STATE FOR THE LIGHT, ie I need to figure out adding a triggeredOff state. Also I should
discuss with the others exactly what needs to be done in this situation.

-DONE: I NEED TO MAKE BASE STATION CONNECTION AND RECONNECTION ATTEMPTS KEEP TRYING UNTIL A BASE STATION CONNECTS, instead of only once
and then not continuing to try if the connection is unsuccessful. If will make a queue of light modules that need to be connected but 
have not yet been connected...

'''

class lightModuleClient:
    def __init__(self, connid, actualState, actualName, actualCurrentTime):
        self.wifiState = None #"OFF" is off, "ON" is on, None is disconnected from base station
        self.actualState = actualState #the actual current state of the light, "OFF" or "ON"
        self.connectionStatus = "NOTYETCONNECTED" #whether connected to base station; can be "NOTYETCONNECTED", "CONNECTED", or "DISCONNECTED"

        self.connid = connid #the ID number of the light
        
        self.wifiName = None #the name of the light module as recommended by the wifi; None if the wifi is disconnected or has not yet tried to change the name
        self.actualName = actualName #the current actual currently stored name of the light module
        self.actualCurrentTime = actualCurrentTime #the time the light has been on for
        
        self.lastConnectionAttemptTime = 0#The time of the last attempt to connect to the base station

        print("    Light ", self.connid, " is NOT YET CONNECTED.")

    def connect(self):#light becoming connected to a base station
        self.connectionStatus = "CONNECTED"
        self.wifiState = self.actualState #start wifiState to be the same as the current actualState
        print("    Light ", self.connid, " is now CONNECTED.")

    def disconnect(self):#light becoming disconnected from a base station
        self.connectionStatus = "DISCONNECTED"
        self.wifiState = None
        print("    Light ", self.connid, " is currently DISCONNECTED.")

    def changeWifiName(self, newName):#change of name has been requested
        self.wifiName = newName
        print("    Light ", self.connid, " is now WIFI NAMED ", newName)

    def changeActualName(self, newName):#the actual name of the light has been changed
        self.actualName = newName

    def confirmNameChange(self, newName):#check if the actual name matches the name requested by wifi
        if self.wifiName == self.actualName:
            return True
        return False

    def changeWifiState(self):#change the wifi recommended state of the light
        if self.wifiState == "OFF":
            self.wifiState = "ON"
            print("    Light ", self.connid, " is now WIFI ON.")
        else:
            self.wifiState = "OFF"
            print("    Light ", self.connid, " is now WIFI OFF.")

    def changeActualState(self, state):#change the actual state of the light
        self.actualState = state

    #yet to be properly implemented using the self.actualLightState variable in the wifiCommunicator class
    #returns [True/False for whether it is confirmed, self.actualState]
    def confirmState(self):#if the server is making sure that the light is on, ensure the light is on
        if self.wifiState == self.actualState:
            print("    Light ", self.connid, " is now CONFIRMED ", self.actualState)
            return [True, self.actualState]
        return [False, self.actualState]

    #def closeLight(self):#This is no longer relevant
    #    print("    Light ", self.connid, "is now OFFLINE.")


class wifiCommunicator():
    #initialStateList is in the format [[actualState, actualName, actualCurrentTime], ... repeated for each light]
    def __init__(self, selector, initialStateList):
        self.sel = selector
        self.lightModuleDict = {}
        self.num_conns = len(initialStateList)
        self.host = "192.168.4.1"
        self.port = int("50007")
        self.start_connections(initialStateList)

    #THIS HAS TO BE COMPLETELY REDONE TO REATTEMPT CONNECTION IF UNSUCCESSFUL THE FIRST TIME!!!!!!!!
    def start_connections(self, initialStateList):
        server_addr = (self.host, self.port)
        for i in range(0, self.num_conns):
            connid = i + 1
            print("attempting connection", connid, "to", server_addr)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setblocking(False)
            sock.connect_ex(server_addr)
            events = selectors.EVENT_READ | selectors.EVENT_WRITE
            data = types.SimpleNamespace(
                connid=connid,
                #msg_total=sum(len(m) for m in messages),
                #recv_total=0,
                messages=[],#list(messages),
                outb=b"",
            )
            self.sel.register(sock, events, data=data)
            self.lightModuleDict[connid] = lightModuleClient(connid, initialStateList[i][0], initialStateList[i][1], initialStateList[i][2])
            self.lightModuleDict[connid].lastConnectionAttemptTime = time.time()#record the time this connection attempt was made

    #reattempt to connect to a light module if it was not able to connect to base station (the old socket must first be unregistered)
    def attemptReconnection(self,connid):
        server_addr = (self.host, self.port)
        print("reattempting connection", connid, "to", server_addr)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)
        sock.connect_ex(server_addr)
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        data = types.SimpleNamespace(
            connid=connid,
            #msg_total=sum(len(m) for m in messages),
            #recv_total=0,
            messages=[],#list(messages),
            outb=b"",
        )
        self.sel.register(sock, events, data=data)
        self.lightModuleDict[connid].lastConnectionAttemptTime = time.time()#record the time this connection attempt was made

    def service_connection(self, key, mask):
        sock = key.fileobj
        data = key.data
        lightModule = self.lightModuleDict[data.connid]
        
        #in addition to the "if not recv_data", might need an try except statement for when the base station disconnects from the piui...
        
        if mask & selectors.EVENT_READ:
            recv_data = sock.recv(1024)  # Should be ready to read
            if recv_data:
                print("received", repr(recv_data), "from connection", data.connid)
                #data.recv_total += len(recv_data)
                if (recv_data == b"CHANGE STATE"):
                    #turn the light on or off
                    lightModule.changeWifiState()

                    '''THIS IS OLD AND NO LONGER NEEDED, we must now only change wifiState and then only confirm the light is on when it has actually been turned on
                    #ACTUALLY NEVERMIND SOMETHING LIKE THIS IS GOOD TO HAVE AS SOON AS THE STATE IS CHANGED...
                    if lightModule.wifiState == "OFF":
                        data.messages += [b"TURNED OFF"]
                    else:
                        data.messages += [b"TURNED ON"]
                    '''
                '''THIS WHOLE SECTION MSUST BE REMADE WITH THE SINGLE NEW CONFIRMSTATE FUNCTION
                if (recv_data == b"CONFIRM ON"):
                    lightModule.confirmOn()
                    data.messages += [b"CONFIRMED ON"]
                if (recv_data == b"CONFIRM OFF"):
                    lightModule.confirmOff()
                    data.messages += [b"CONFIRMED OFF"]
                '''
                if (recv_data == b"CONFIRM STATE"):
                    stateConfirmation = lightModule.confirmState()
                    if stateConfirmation[0] == False:#if the light has not yet changed state
                        if stateConfirmation[1] == "ON":
                            data.messages += [b"STATENOTCHANGED_ON"]
                        else:
                            data.messages += [b"STATENOTCHANGED_OFF"]
                    else:#if the light has successfully changed state
                        if stateConfirmation[1] == "ON":
                            data.messages += [b"STATECHANGED_ON"]
                        else:
                            data.messages += [b"STATECHANGED_OFF"]
                        
                if (recv_data == b"CONNECTED"):
                    lightModule.connect()
                if (recv_data[0:7] == b"CHANGEN"):#full command is CHANGENAME_newName
                    lightModule.changeWifiName(recv_data[11:])#set the name of the light to the name in the wifi message
                if (recv_data[0:11]==b'CONFIRMCHANG'):#full command is CONFIRMCHANGENAME_newName
                    if lightModule.confirmNameChange(recv_data[17:]) == False:#check whether the light name has been changed
                        data.messages += [b"NAMENOTCHANGED"]#confirm that the name has not been changed
                    else:
                        data.messages += [b"NAMECHANGED"]#confirm that the name has been changed
            if not recv_data: #or data.recv_total == data.msg_total: #if it gets disconnected from the base station
                print("closing socket", data.connid)
                self.sel.unregister(sock)
                sock.close()
                lightModule.disconnect()#properly disconnect the light module socket
                #self.lightModuleDict.pop(data.connid)#we are no elimintating the light module when the base station gets disconnected
                self.attemptReconnection(data.connid)#attempt to reconnect the light module
        
        #if the current light module has passed 2 seconds since attempting to connect to base station without response, reattempt connection
        #nevermind... the below if statement should not be used
        #if not data.messages:#must check to make sure that no incoming messages were just received

        #if the light is currently disconnected and the last attempt to connect was greater than 2 seconds ago...
        if (lightModule.connectionStatus == "NOTYETCONNECTED" or lightModule.connectionStatus == "DISCONNECTED") and time.time()-lightModule.lastConnectionAttemptTime > 2:
            print("closing socket", data.connid)
            self.sel.unregister(sock)
            sock.close()
            lightModule.disconnect()#properly disconnect the light module socket
            self.attemptReconnection(data.connid)

        if mask & selectors.EVENT_WRITE:
            if not data.outb and data.messages:
                data.outb = data.messages.pop(0)
            if data.outb:
                print("sending", repr(data.outb), "to connection", data.connid)
                sent = sock.send(data.outb)  # Should be ready to write
                data.outb = data.outb[sent:]
    '''
    This function returns the state of the light wifi command in the light dict with the highest connID on this pi0
    (obviously there would be usually only 1 light module for a given pi0... but this format is useful for testing)

    ####################################
    ACTUALLY SHOULD CHANGE IT TO WORK WITH THE LIGHT OF CONNID=1 FOR SIMPLICITY
    ####################################

    Outputs:
        - None if there are no light modules initialized
        - State if there is a light module, a list with the following elements in order: 
            -"CONNECTED"/"NOTYETCONNECTED"/"DISCONNECTED"
            -"ON"/"OFF"
            -nameOfLight
        where nameOfLight is the name which the wifi is requesting that the lightModuleBeNamed

    Note that we have not dealt with the edge case of the piui disconnecting... ie ["DISCONNECTED", "ON"]
    ''' 
    def getState(self):#REMAKE THIS FUNCTION BASED ON THE NEW LIGHTMODULE MODIFICATIONS
        '''
        state = None
        highestConnID = -1
        for id in self.lightModuleDict:#iterate through each light module on this pi0 
            if id > highestConnID: #if we have found a light with a new higher connid than previously... update the state with info for this light
                if lightModule.wifiState is None:
                    state = ["DISCONNECTED", "OFF", lightModule.wifiName]
                elif lightModule.wifiState == 1:
                    state = ["CONNECTED", "ON", lightModule.wifiName]
                elif lightModule.wifiState == 0:
                    state = ["CONNECTED", "OFF", lightModule.wifiName]

        '''
        if 1 not in self.lightModuleDict:
            return None
        lightModule = self.lightModuleDict[1]
        return [lightModule.connectionStatus, lightModule.wifiState, lightModule.wifiName]

    
    '''
    Tells the wifiCommunicator class about the actual state of the light

    The wifi's response to this has not yet been implemented.

    Input argument: Can be one of ["ON", nameOfLight, currentTime, triggeredOFF]
    where nameOfLight is the name of the light, currentTime is the currentTime the light has been on for, and 
    triggeredOFF is boolean whether the motion sensor has been triggered
    '''
    def confirmState(self, actualLightState):#REMAKE THIS FUNCTION BASED ON THE NEW LIGHTMODULE MODIFICATIONS
        #something along the lines of self.actualLightState = actualLightState
        stateInput = actualLightState[0]
        nameInput = actualLightState[1]
        if 1 not in self.lightModuleDict:#check if the light modules have been initialized yet
            return None
        else:
            lightModule = self.lightModuleDict[1]
            if stateInput == "ON":#set the light module's actual state to match what the main loop program on the pi0 is saying
                lightModule.changeActualState("ON")
            elif stateInput == "OFF":
                lightModule.changeActualState("OFF")
            lightModule.changeActualName(nameInput)#set the light module's actual name to match what the main loop program is saying            

    def checkWifi(self):
        try:
            events = self.sel.select(timeout=1)
            if events:
                for key, mask in events:
                    self.service_connection(key, mask)
            # Check for a socket being monitored to continue.
            if not self.sel.get_map():
                return
        except KeyboardInterrupt:
            print("caught keyboard interrupt, exiting")