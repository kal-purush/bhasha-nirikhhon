import util

class Configuration:              # class to store values into Queue
    def __init__(self, CONF_PROJECT_FILENAME, CONF_BOX_FILENAME):
        # default configs, in case of error reading the config file
        self.box_id  = 99999
        self.sensors = []
        self.send_interval = 10
        
        self.SERVER_URL = "http://server.com/api/send-data/"
        self.CHECK_CONNECTION_URL = "http://server.com/api/"
        self.VERSION = 1.0
        self.ENCRYPTION_KEY = "abcdefghijkmnopkrstubwxyz0123456"
        
        self.CONF_PROJECT_FILENAME = CONF_PROJECT_FILENAME
        self.CONF_BOX_FILENAME  = CONF_BOX_FILENAME
    
    def load_config_values(self):
        """
        Loads the Configuration values from both the client_config.json as the ss-box_config.json into the global Vars
        """
        # -------------------   Box Config   -----------------
        value = util.read_json_config(self.CONF_BOX_FILENAME, 'box_id')
        if not value is False:       # can't compare == True, because it's not a boolean when the value was read correctly
            self.box_id = value

        value = util.read_json_config(self.CONF_BOX_FILENAME, 'sensors')
        if not value is False:       # can't compare == True, because it's not a boolean when the value was read correctly
            self.sensors = value

        value = util.read_json_config(self.CONF_PROJECT_FILENAME, 'SEND_INTERVAL')
        if not value is False:       # can't compare == True, because it's not a boolean when the value was read correctly
            self.send_interval = value

        # -------------------   Project Config   -----------------
        value = util.read_json_config(self.CONF_PROJECT_FILENAME, 'SERVER_URL')
        if not value is False:       # can't compare == True, because it's not a boolean when the value was read correctly
            self.SERVER_URL = value

        value = util.read_json_config(self.CONF_PROJECT_FILENAME, 'CHECK_CONNECTION_URL')
        if not value is False:       # can't compare == True, because it's not a boolean when the value was read correctly
            self.CHECK_CONNECTION_URL = value
        
        value = util.read_json_config(self.CONF_PROJECT_FILENAME, 'VERSION')
        if not value is False:       # can't compare == True, because it's not a boolean when the value was read correctly
            self.VERSION = value
            
        value = util.read_json_config(self.CONF_PROJECT_FILENAME, 'ENCRYPTION_KEY')
        if not value is False:       # can't compare == True, because it's not a boolean when the value was read correctly
            self.ENCRYPTION_KEY = value
            
        # Print loaded CONFIG
        print('BOX_ID                =', self.box_id)

        print('SERVER_URL            =', self.SERVER_URL)
        print('CHECK_CONNECTION_URL  =', self.CHECK_CONNECTION_URL)
        print('VERSION               =', self.VERSION)
        print('ENCRYPTION_KEY        =', self.ENCRYPTION_KEY)
        print('SEND_INTERVAL         =', self.SEND_INTERVAL)

        print "sensors"
        for sensor in self.sensors:
            print sensor['name'], sensor['description']