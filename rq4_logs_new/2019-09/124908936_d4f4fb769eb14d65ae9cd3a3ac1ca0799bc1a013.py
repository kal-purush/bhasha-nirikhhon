from wx import Button, EVT_BUTTON


class SimpleButton(Button):
    """
    Button widget
    """
    def __init__(self, parent, callback, text_button:str, size=None):
        """
        Build widget, bind callback
        :param parent: Parent wx element
        :param callback: Function called on click
        :param text_button: Button text
        """
        if size:
            super().__init__(parent, label=text_button, size=size)
        else:
            super().__init__(parent, label=text_button)
        self.Bind(EVT_BUTTON, callback)