#!/usr/bin/env python
import rospy
from hbba_msgs.msg import Desire, Event
from hbba_msgs.srv import AddDesires, RemoveDesires
from scenario_manager_action_server import ScenarioManagerAction
#from states import state_00, state_01, state_02, state_03, state_04, state_05, state_06, state_07, state_08, state_09, state_10, state_11, state_12

class Scenario2Manager(ScenarioManagerAction):

    def __init__(self):
        ScenarioManagerAction.__init__(self, name="scenario_2_manager")
        self.desires = {}
        self.states = {}
        self.current_state = None
        self.reaction_events = [Event.ACC_ON, Event.ACC_OFF, Event.IMP_ON, Event.IMP_OFF]
        # Get and add all states
        self.add_state(state_00.State00(self.desires))
        self.add_state(state_01.State01(self.desires))
        self.add_state(state_02.State02(self.desires))
        self.add_state(state_03.State03(self.desires))
        self.add_state(state_04.State04(self.desires))
        self.register_preempt_callback(self.canceled_cb)
        self.rem_desires = rospy.ServiceProxy('remove_desires', RemoveDesires)
        rospy.wait_for_service("remove_desires")

    def add_state(self, state):
        print("keys")

        key = state.get_id()

        print("key")

        if key not in self.states.keys():
            print("keys")
            self.states[key] = state
            print("state")

            if self.current_state is None:
                print("none")
                self.current_state = self.states.get(key)

    def observe(self):
        self.sub_desires = rospy.Subscriber("events", Event, self.eventCB, queue_size=5)

    def stopObserving(self):
        self.sub_desires.unregister()

    def eventCB(self, event):
        # Update own desire states w/ events seen
        if event.desire in self.desires:
            if event.type in self.reaction_events:
                self.desires[event.desire] = event.type

                react_result = self.states[self.current_state].react_to_event()

                if react_result is not None:
                    self.states[self.current_state].cleanup()

                    if react_result == "Done":
                        self._result.result = True
                        self._as.set_succeeded(self._result)

                        self.current_state = "state_00"
                        self.stopObserving()
                        
                    else:
                        self._feedback.prev_state = self.current_state
                        self.current_state = react_result

                        self._feedback.state = react_result
                        self._as.publish_feedback(self._feedback)

                        self.states[self.current_state].add_desires()
    
    def execute_cb(self, goal):
        if goal.execute is True:
            self.observe()
            # initial desire addition
            self.states[self.current_state].add_desires()
        else:
            pass
            # no stuff

    def canceled_cb(self):
        for desire in self.desires:
            self.rem_desires.call(desire)
        self.desires.clear()
        self.current_state = "state_00"
        self._as.set_preempted()
        self.stopObserving()

class Scenario1Tester:
    def __init__(self):
        client = actionlib.SimpleActioClient("sc1tester", custom_msgs.msg.scenario_managerAction)
        client.wait_for_server()
        goal = custom_msgs.msg.scenario_managerGoal(execute=True)
        client.send_goal(goal)


if __name__ == "__main__":
    try:
        rospy.init_node("scenario_1_manager")

        node = Scenario1Manager()
        doTest = Scenario1Tester()

        rospy.spin()

    except rospy.ROSInterruptException:
        pass