#!/usr/bin/python

########################
#Payay Bot version 0.01#
#You are free to modify#
#the sourcecode to your#
#       likings!       #
########################

import os;
import importlib;
import sys;
import inspect;
import ConfigParser;
import time;

curFolder = os.path.dirname(os.path.join(sys.path[0], os.path.basename(os.path.realpath(sys.argv[0]))));
pluginFolder = curFolder + "\\plugins";
pluginList = {};
configName = sys.argv[1];
doLoop = True;
ircObject = None;

def initPlugins(configName):
    for plugin in os.listdir(pluginFolder):
        if plugin[-3:] == ".py" or plugin[-4:] == ".pyc":
            if plugin[:-3] == "__init__" or plugin[:-4] == "__init__":
                continue;
            else:
                if plugin[-4:] == ".pyc":
                    exten = 0;
                    pluginName = plugin[:-4];
                else:
                    if os.path.isfile(pluginFolder + "/" + plugin + "c") != False:
                        continue;
                    exten = 1;
                    pluginName = plugin[:-3];

            print("Initializing " + pluginName);
            pluginList[pluginName] = importlib.import_module("plugins." + pluginName);
            getattr(pluginList[pluginName], "init")(configName, curFolder);
    if len(pluginList) == 1:
        thing = "plugin";
    else:
        thing = "plugins";
    print("Initialized " + str(len(pluginList)) + " " + thing +"!");

def initConfig(configName):
    configFull = curFolder + "/" + configName;
    if os.path.isfile(configFull) == False:
        print("Creating config file!");
        config = open(configFull, "w+");

def initLoop():
    #TODO IRC

    while doLoop == True:
        for plugin in pluginList:
            getattr(pluginList[plugin], "tick")();
    time.wait(1/30);

def initSettings(configName):
    config = curFolder + "/" + configName;

if __name__ == "__main__":
    initConfig(configName);
    initPlugins(configName);
    #initSettings()
    initLoop();