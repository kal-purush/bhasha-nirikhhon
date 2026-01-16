class Add_Word:

    def __init__(self, input_manager):
        self.input_manager = input_manager

    def run(self, parent):  # Add word to user text when button is clicked
        user_input = self.input_manager.string
        index = len(user_input) - 1

        if len(user_input) > 0 and user_input[index] == " ":
            self.input_manager.string += parent.text_box.text
        else:
            self.input_manager.string += " " + parent.text_box.text