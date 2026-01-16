
from hardware.serialHardware import Hardware
from aplications.baseClass import baseClass
from dispatcher.commandTable import commands
from dispatcher.dispatcher import dispatcher
import logging
import array
import MySQLdb
import datetime
import time

class app(baseClass):
    def __init__(self,name):
        baseClass.__init__(self,name,False)
        
        self._pipe_list = [0]
        self.i = 0
        
        self._old_load = {}
        self._old_load["main"] = None
        self._old_load["water"] = None
        self._old_load["heating_week"] = None
        self._old_load["heating_weekend"] = None
        pass
   
    ## 
    # @base_virtual
    def virtual_new_data(self, dispatcher, pipe, command, payload):
        table = dispatcher.command_table()
        self._command_table[table.IDLE] = self._idle_data
        self._command_table[table.HANDLE_GET_SCREENS] = self._new_get_screens        
        self._command_table[table.HANDLE_HOME_TEMPERATURE] = self._new_home_temperature
        self._command_table[table.HANDLE_RELOAD_HEATING_SCREEN_WEEK] = self._new_reload_screen
        self._command_table[table.HANDLE_RELOAD_HEATING_SCREEN_WEEKEND] = self._new_reload_screen
        self._command_table[table.HANDLE_RELOAD_WATER_SCREEN] = self._new_reload_screen
        self._command_table[table.HANDLE_RELOAD_MAIN_SCREEN] = self._new_reload_screen
        self._command_table[table.HANDLE_PROGRAM_MANUAL] = self._new_program_selection
        self._command_table[table.HANDLE_WATER_SCREEN] = self._new_water_screen
        self._command_table[table.HANDLE_HEATING_SCREEN] = self._new_heating_screen
        
        
        self._command_handler(dispatcher,pipe,command,payload)
        
    ## 
    # @base_virtual
    def virtual_tx_finished(self, dispatcher, pipe):
        pass
    
    ## 
    # @base_virtual    
    def virtual_tx_failed(self, dispatcher, pipe, command, data):
        pass

    ## 
    # @base_virtual
    def virtual_error(self, dispatcher, error_code):
        pass
        
    def _idle_data(self,send,table,pipe,command,load):
        if self.i == 10:
            self._send_main_screen(send, table, pipe,True)
            self._send_water_screen(send, table, pipe,True)
            self._send_heating(send, table, pipe,True,True)
            self._send_heating(send, table, pipe,False,True)
            
            self.i = 0
            
        self.i += 1
        pass
    
    def _new_water_screen(self,send,table,pipe,command,load):
        self._log.debug("new water screen")
        #nastavit databazi pro vodu
        lst = load
        self._log.debug(str(lst))
        
        c = 0
        con = self.database_connect()
        cur = con.cursor()
        for a in range(0,8,4):
            start = datetime.time(lst[a],lst[a+1]) 
            stop = datetime.time(lst[a+2],lst[a+3])
            query = "call sp_configureProgram(2,%f,\"%s\",\"%s\",NULL,%d)" %(lst[-1]/2,start,stop,c)
            c += 1
            self._log.debug(query)
            cur.execute(query)
            
        con.close()
        pass
    
        
    def _new_heating_screen(self,send,table,pipe,command,load):
        self._log.debug("new heating screen")
        lst = load
        self._log.debug(str(lst))
        
        c = 0
        if len(lst) == 12:
            weekend = False
        elif len(lst) == 6:
            weekend = True
        else:
            self._log.warning("new heating screen wrong packet length")
            return
        
        con = self.database_connect()
        cur = con.cursor()
            
        for a in range(0,len(lst),3):
            start = datetime.time(lst[a],lst[a+1]) 
            query = "call sp_configureProgram(3,%f,\"%s\",NULL,%d,%d)" %(lst[a+2]/2,start,weekend,c)
            c += 1
            self._log.debug("heating screen query: " +query)
            cur.execute(query)    
        #nastavit databazi pro topeni 
        
        con.close()
        pass
    
    def _new_program_selection(self,send,table,pipe,command,load):
        if len(load) != 2:
            return
                
        t = load[0] / 2.0
        p = load[1]        
        self._log.info("new program %d, manual temp %f" %(p,t))
        
        query = "call sp_selectProgram(%d)" % p
        self._log.debug(query)
        q2 = "update programy set teplota = %f where id = 1" % t
        self._log.debug(q2) 

        con = self.database_connect()
        cur = con.cursor()
        cur.execute(q2)
        cur.execute(query)
        con.close()
    
    def _new_reload_screen(self,send,table,pipe,command,load):
        if (command == table.HANDLE_RELOAD_HEATING_SCREEN_WEEK):
            self._send_heating(send, table, pipe, False)
            pass
        elif (command == table.HANDLE_RELOAD_HEATING_SCREEN_WEEKEND):
            self._send_heating(send, table, pipe, True)
            pass
        elif (command == table.HANDLE_WATER_SCREEN):
            self._send_water_screen(send, table, pipe)
            pass
        elif (command == table.HANDLE_MAIN_SCREEN):
            self._send_main_screen(send, table, pipe)
            pass
    
    def _new_get_screens(self,send,table,pipe,command,load):
        self._send_main_screen(send, table, pipe)
        self._send_water_screen(send, table, pipe)
        self._send_heating(send, table, pipe,True)
        self._send_heating(send, table, pipe,False)
        pass
    
    def _new_home_temperature(self,send,table,pipe,command,load):
        if len(load) == 2:
            t = self.getInt(load)/2.0
            if (t < 100):
                self._log.info("new water temperature received %f", t)
                self._log_event_to_db(pipe,table,t,table.event_id.HOME_TEMPERATURE, tolerance=True)
            else:
                self._log.warning("water temerature greater then 100 (%f)",t)
    
    def _send_screen(self,send,pipe, command, string, if_changed = False):
        load = array.array("B")
        
        if (string == "main"):
            load.append(time.localtime().tm_hour)
            load.append(time.localtime().tm_min)
            load.append(time.localtime().tm_wday)    
        
        d = self.database_connect()
        assert isinstance(d, MySQLdb.connection)
        cur = d.cursor()
        assert isinstance(cur,MySQLdb.cursors.Cursor)
                                    
        query =  "select value from temp_screens_table where screen = \"%s\" order by idx asc" % string
        cur.execute(query )
        d.close()
        
        row = cur.fetchone()
        i = 0
        while row:
            load.append(row[0])
            row = cur.fetchone()
            i += 1
            ## home temperature is not in database
            if (i == 3 and string == "main"):
                load.append(0)

        if (self._old_load[string] != load or if_changed is False):
            self._log.info("sent screen data for screen: %s (%s)" % (string , str(load)))
            send(pipe, command,load )
            ## save old status
            self._old_load[string] = load
        
    def _send_heating(self,send,table,pipe, weekend, if_changed = False):
        if weekend:
            stra = "heating_weekend"
            
        else:
            stra = "heating_week"
        
        self._send_screen(send,pipe,table.HANDLE_HEATING_SCREEN,stra,if_changed)
                
    def _send_water_screen(self,send,table,pipe, if_changed = False):
        self._send_screen(send,pipe,table.HANDLE_WATER_SCREEN,"water",if_changed)
        
    def _send_main_screen(self, send,table,pipe, if_changed = False):
        self._send_screen(send,pipe,table.HANDLE_MAIN_SCREEN,"main",if_changed)
        