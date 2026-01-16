import core


def test_app():
    input_values = ['ring', 'Gimli', 'ring']
    output = []

    def mock_input(s):
        output.append(s)
        return input_values.pop(0)

    core.input = mock_input
    core.print = lambda s: output.append(s)

    core.main()

    assert output == [
        'My lord, create a new master password please! >',
        '<--Greetings you!-->',
        'What is your name stranger? > ',
        'I need a key to save your secrets... > ',
        '\n<--Welcome back master Gimli!-->\n',
    ]