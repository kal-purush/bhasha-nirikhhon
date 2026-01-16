from Apps.app import App


class FakeActivity(App):
    time_on = 0
    transition_counter_in = 0
    transition_counter_out = 0

    def __init__(self, time_on):
        App.__init__(self, "Fake Activity")
        self.time_on = time_on

    def update(self, sys):
        if not sys.sensors["Presence Sensor"].value:
            if self.transition_counter_in == self.time_on - 1:
                self.transition_counter_in = 0
                if not sys.devices["Indoor Lights"].value:
                    return [{"device": "Indoor Lights", "target": "on"}], [[0, 0, 8]], [], [], []
                else:
                    return [{"device": "Indoor Lights", "target": "off"}], [[0, 0, 8]], [], [], []
            else:
                self.transition_counter_in += 1

            if self.transition_counter_out == self.time_on - 1:
                self.transition_counter_out = 0
                if not sys.devices["Outdoor Lights"].value:
                    return [{"device": "Outdoor Lights", "target": "on"}], [[0, 0, 8]], [], [], []
                else:
                    return [{"device": "Outdoor Lights", "target": "off"}], [[0, 0, 8]], [], [], []
            else:
                self.transition_counter_out += 1
        return [], [], [], [], []