from kivy.app import App
from kivy.lang import Builder
from kivy.properties import NumericProperty
from kivy.properties import StringProperty
from kivy.uix.boxlayout import BoxLayout
from kivy.clock import Clock
from playsound import playsound
from datetime import datetime, timedelta

Builder.load_file("main.kv")


class MainWidget(BoxLayout):
    number = NumericProperty()
    timestr = StringProperty()

    def __init__(self, **kwargs):
        super(MainWidget, self).__init__(**kwargs)
        Clock.schedule_interval(self.increment_time, .1)
        self.increment_time(0)
        self.stop()
        self.beep()
        self.number = 0
        self.init = 0

    def beep(self):
        self.exam_dur = 100
        if self.number >= self.exam_dur:
            if self.init == 0:
                for _ in range(3):
                    playsound("data/beep.mp3")

                self.init = 1

    def increment_time(self, interval):
        self.number += .1

        sec = timedelta(seconds=int((self.number)))
        time = datetime(1, 1, 1) + sec
        self.timestr = time.strftime("%M:%S")

        self.beep()

    def start(self):
        Clock.unschedule(self.increment_time)
        Clock.schedule_interval(self.increment_time, .1)

    def stop(self):
        Clock.unschedule(self.increment_time)
        self.init = 0


class ExampleApp(App):
    def build(self):
        return MainWidget()

ExampleApp().run()