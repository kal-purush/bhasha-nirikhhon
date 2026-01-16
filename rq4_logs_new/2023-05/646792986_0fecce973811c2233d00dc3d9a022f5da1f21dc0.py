from kivy.app import App
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.button import Button
from kivy.uix.textinput import TextInput
from kivy.uix.label import Label
from math import sqrt


class CalculatorApp(App):
    def build(self):
        self.title = "PindriX Calculator"
        layout = BoxLayout(orientation='vertical', spacing=10)

        self.result_label = TextInput(
            text="0",
            font_size='48sp',
            readonly=True,
            size_hint=(1, 0.3),
            background_color=(1, 1, 1, 0.8),
            foreground_color=(0, 0, 0, 1),
        )
        layout.add_widget(self.result_label)

        buttons = [
            ['7', '8', '9', '/'],
            ['4', '5', '6', '*'],
            ['1', '2', '3', '-'],
            ['C', '0', '=', '+'],
            ['(', ')', '√', 'x^2'],
        ]

        grid_layout = BoxLayout(orientation='vertical', spacing=10)

        for row in buttons:
            row_layout = BoxLayout(spacing=10)

            for button_text in row:
                button = Button(
                    text=button_text,
                    font_size='24sp',
                    background_color=(0.2, 0.7, 0.9, 1),
                    on_press=self.on_button_press
                )
                row_layout.add_widget(button)

            grid_layout.add_widget(row_layout)

        layout.add_widget(grid_layout)

        return layout

    def on_button_press(self, instance):
        button_text = instance.text

        if button_text == 'C':
            self.result_label.text = '0'
        elif button_text == '=':
            try:
                self.result_label.text = str(eval(self.result_label.text))
            except ZeroDivisionError:
                self.result_label.text = 'Error'
        elif button_text == '√':
            try:
                self.result_label.text = str(sqrt(eval(self.result_label.text)))
            except ValueError:
                self.result_label.text = 'Error'
        elif button_text == 'x^2':
            self.result_label.text = str(eval(self.result_label.text) ** 2)
        else:
            if self.result_label.text == '0':
                self.result_label.text = button_text
            else:
                self.result_label.text += button_text


if __name__ == '__main__':
    CalculatorApp().run()