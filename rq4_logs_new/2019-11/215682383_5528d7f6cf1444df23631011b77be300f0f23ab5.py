import rospy
import smach
import random
# import depthPub
# import depthSub
# import sonarSub

class SEARCHPATH(smach.State):
    def __init__(self):
        smach.State.__init__(self, outcomes =['Path found', 'Path not found', 'Failed'],
                                    input_keys=['depth_input', 'path_miss_in'],
                                    output_keys=['depth_output','path_miss_out'])
        

    def execute(self, userdata):
        self.path_miss = userdata.path_miss_in
        self.cv_found = self.cv()
        #rospy.loginfo(self.cv_found)
        
        if(self.cv_found == 1):
            rospy.loginfo('Path Found')
            return 'Path found'
        
        if(self.cv_found == 0):
            #rospy.loginfo('Path not Found')
            self.path_miss +=1
            userdata.path_miss_out = self.path_miss
            
            if(self.path_miss > 2):
                rospy.loginfo('Path not found for > 3')
                return 'Failed'
            #rospy.loginfo(self.path_miss)
            rospy.loginfo('Path not found')
            return 'Path not found'

    def cv(self):
        rd = random.randint(0,100)
        return (rd%2)

class FOLLOWPATH(smach.State):
    def __init__(self):
        smach.State.__init__(self, outcomes = ['Object detected', 'Object not found'],
                                    input_keys= ['distance_in', 'path_miss_in'],
                                    output_keys=['path_miss_out'])
    
    def execute(self, userdata):
        self.path_followed = self.followed()
        self.dist = userdata.distance_in

        if (self.path_followed == 1):
            while(self.dist >0):
                self.dist -= 1

            if(self.detect() == 1):
                
                rospy.loginfo('Object detected')
                return 'Object detected'
                

        self.path_miss = userdata.path_miss_in
        self.path_miss += 1
        self.path_miss_out = self.path_miss
        return 'Object not found'

    def followed(self):
        rd = random.randint(0,100)
        return (rd%2)
    
    def detect(self):
        rd = random.randint(0,100)
        return (rd%2)

class RETURN(smach.State):
    def __init__(self):
        smach.State.__init__(self, outcomes = ['Returned'], input_keys=['distance_in'])
        
    def execute(self, userdata):
        rospy.loginfo('Turning around')
        self.dist = userdata.distance_in
        while(self.dist >0):
            self.dist -= 1
        return 'Returned'

class SURFACE(smach.State):
    def __init__(self):
        smach.State.__init__(self, outcomes = ['Surfaced'],  input_keys=['depth_input'],
                                                            output_keys=['depth_output'])
        
    def execute(self, userdata):
        self.dep = userdata.depth_input
        while(self.dep >0):
            self.dep -= 1
        userdata.depth_output = self.dep
        return 'Surfaced'

def main():
    rospy.init_node('smach_path_task')

    sm = smach.StateMachine(outcomes=['object_detected', 'surfaced'])

    # depth_sub = depthSub()
    # depth_pub = depthPub(10)
    # sonar_sub = sonarSub()
    # depth_pub.talker()

    sm.userdata.sm_distance = 15
    sm.userdata.sm_depth =10
    sm.userdata.sm_sonarDetection = 0
    sm.userdata.sm_path_miss = 0
    
    
    with sm:
        #Searc for pat
        smach.StateMachine.add('SEARCHPATH', SEARCHPATH(), transitions={'Path found':'FOLLOWPATH',
                                                                        'Path not found': 'RETURN',
                                                                        'Failed':'SURFACE'},
                                                            remapping ={'depth_input':'sm_depth',
                                                                        'depth_output':'sm_depth',
                                                                        'path_miss_in':'sm_path_miss',
                                                                        'path_miss_out': 'sm_path_miss'})
        #follow te pat
        smach.StateMachine.add('FOLLOWPATH', FOLLOWPATH(), transitions={'Object detected':'object_detected',
                                                                        'Object not found':'RETURN'},
                                                            remapping={'distance_in':'sm_distance',
                                                                        'path_miss_in':'sm_path_miss',
                                                                        'path_miss_out': 'sm_path_miss'})
        #retr to pat to searc for pat agai
        smach.StateMachine.add('RETURN',RETURN(), transitions={'Returned':'SEARCHPATH'},
                                                remapping={'distance_in':'sm_distance'})
        #srface 
        smach.StateMachine.add('SURFACE', SURFACE(), transitions={'Surfaced':'surfaced'},
                                                    remapping ={'depth_input':'sm_depth',
                                                                    'depth_output':'sm_depth'})

    outcome = sm.execute()

if __name__ == '__main__':
    main()