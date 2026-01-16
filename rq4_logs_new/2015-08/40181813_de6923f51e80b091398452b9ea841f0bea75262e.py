'''
Created on 18 juil. 2014
Last change on Oct. 23, 2014

@author: Javier
'''
import Speech
import Behavior

import time

from Config import *

class BML_controler:
    '''
    @Class: Intermediate step between BML and the execution of the action. We also manage the static parameters here.
               
    @attribute self.my_speech: from where the speeches will be handled and executed.
    @attribute self.my_behavior: from where the behaviors will be handled executed.
    '''
    def __init__(self):
        if(DEBUG_MODE): print("init BML_controler")
        self.my_speech = Speech.Speech()
        self.my_behavior = Behavior.Behavior()

    '''
    @method perform_speech: Perform the speeches (text or mp3) (when creating the action thread)
    @param speech (for type mp3): is the name of the mp3 file to play
    @param speech (for type text): is the text that will be read through ACAPELA
    @param offset: time to wait before executing the speech
    @param speech_type: "mp3" for playing mp3 and "text" for reading text 
    
    @todo: MOVE WAITING TIME MORE DEEPER (if there is some latency + waiting smarter)
    '''
    def perform_speech(self,speech, offset, speech_type):
        #Not really needed
        if (LAG_SPEECH>0):
            time.sleep(LAG_SPEECH)
            
        if(DEBUG_MODE): print("Waiting for %f sec" % offset)
        time.sleep(offset)
        
        if speech_type == "mp3":
            self.my_speech.play_mp3(speech)
        elif speech_type == "text":
            self.my_speech.read_text(speech)
        else: 
            print("ERROR type : %s" % type)    
    
    '''
    @method perform_behavior: Perform the behaviors (when creating the action thread)
    @param behavior: is the name of the behavior to execute
    @param offset: time to wait before executing the behavior
    @param angle_param: which will be added to some joints to follow a human
    
    @todo: MOVE WAITING TIME MORE DEEPER (if there is some latency + waiting smarter)
    '''
    def perform_behavior(self, behavior, offset, param_behavior):

        if (LAG_BEHAVIOR>0):
            time.sleep(LAG_BEHAVIOR)
        if(DEBUG_MODE): print("Perform behavior: %s" % behavior)
        if(DEBUG_MODE): print("Waiting for %f sec" % offset)
        
        time.sleep(offset)
        self.my_behavior.execute_behavior(behavior, param_behavior)
        
        
        