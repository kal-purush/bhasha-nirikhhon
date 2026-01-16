#!/usr/bin/python2.7
#<--encoding: UTF-8-->


import kivy
kivy.require('1.4.1')       # replace with your current kivy version !

from kivy.app import App    # your main entry point into the Kivy run loop.
#from kivy.uix.floatlayout import FloatLayout
from kivy.properties import ObjectProperty
#from kivy.uix.button import Button

# implement sliding from one screen to other
from kivy.uix.screenmanager import (ScreenManager, Screen, SlideTransition,
                                    WipeTransition)


#from kivy.uix.widget import Widget
#class MyWidget(Widget):
#    pass

class LoginPage(Screen):
    '''Defines the Login page with objects that collect user input.'''

    # Properties should always be class variables, not instance variables
    user_email = ObjectProperty()
    user_password = ObjectProperty()
    login = ObjectProperty()
    sign_up = ObjectProperty()

    def __init__(self, *args, **kwargs):
        '''Binds the Login & Sign Up buttons to on_press event handlers.'''

        super(LoginPage, self).__init__(*args, **kwargs)
        self.login.bind(on_press=self.load_user_workspace)
        self.sign_up.bind(on_press=self.load_sign_up_page)

    def load_user_workspace(self, obj):
        '''Changes the currently displayed screen. '''

        #login_page.clear_widgets()
        self.manager.current = 'workspace'
        self.manager.transition.direction = 'left'

    def load_sign_up_page(self, obj):
        '''Opens up the Sign Up form or redirects to SugarSync website.'''

        print("sign up button pressed %s" % obj)


class UserWorkspace(Screen):
    '''This is the main screen with the user workspace.'''

    def __init__(self, *args, **kwargs):
        super(UserWorkspace, self).__init__(*args, **kwargs)

        def load_sign_up_page(obj):
            self.manager.current = 'login'
            self.manager.transition.direction = 'right'

        pass
        #button = Button(text='Hello', size_hint=(None, None), size=(100, 40))
        #button.bind(on_press=load_sign_up_page)
        #self.add_widget(button)


class SugarSync(App):
    '''Controller class.'''

    def build(self):
        '''Overrides parent's build(). Must return a Widget that will be used
        as the root of the widget tree.'''

        # because I'm experimenting, I chose two types of transitions
        st = SlideTransition()
        #wt = WipeTransition()
        # use one transition type or the other
        sm = ScreenManager(transition=st)
        login_page = LoginPage(name='login')
        sm.add_widget(login_page)
        sm.add_widget(UserWorkspace(name='workspace'))

        return sm
#        return MyWidget()


if __name__ == '__main__':
    SugarSync().run()       # don't forget to instantiate the subclass of App