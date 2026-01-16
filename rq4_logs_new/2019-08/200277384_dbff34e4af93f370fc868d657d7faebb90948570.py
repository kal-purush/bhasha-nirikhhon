from kivy.app import App
from kivy.uix.widget import Widget
from kivy.uix.textinput import TextInput

class AlcOrGas(App):
    textinput = TextInput(text='Hello world')


if __name__ == '__main__':
    AlcOrGas().run()