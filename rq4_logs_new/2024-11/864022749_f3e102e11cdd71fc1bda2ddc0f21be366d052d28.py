from kivy.app import App
from kivy.lang import Builder
from kivy.uix.label import Label


class DynamicLabelsApp(App):
    """App to display a list of names dynamically as Labels"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.names = ["Alice", "Bob", "Charlie", "Diana", "Eve"]

    def build(self):
        """Build the app by loading the kv file and adding labels dynamically"""
        self.title = "Dynamic Labels"
        self.root = Builder.load_file('dynamic_labels.kv')
        self.create_labels()
        return self.root

    def create_labels(self):
        """Create a label for each name in the names list and add it to the layout"""
        for name in self.names:
            temp_label = Label(text=name, font_size=24, color=(1, 1, 1, 1))
            self.root.ids.main.add_widget(temp_label)


DynamicLabelsApp().run()