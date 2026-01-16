from flask import Flask, render_template_string, request
import time
import socket

app = Flask(__name__)

html_template = """
<!DOCTYPE html>
<html>
<head>
    <title>GPIO Control</title>
    <style>
        body {
            background-color: #121212;
            color: #ffffff;
            font-family: Arial, sans-serif;
        }
        h1 {
            text-align: center;
        }
        form {
            text-align: center;
            margin-top: 50px;
        }
        input[type="text"] {
            padding: 10px;
            font-size: 16px;
            margin-bottom: 20px;
        }
        button {
            background-color: #1f1f1f;
            color: #ffffff;
            border: 1px solid #ffffff;
            padding: 10px 20px;
            font-size: 16px;
            cursor: pointer;
        }
        button:hover {
            background-color: #333333;
        }
    </style>
</head>
<body>
    <h1>GPIO Character Sequence Control</h1>
    <form method="POST" action="/control">
        <input type="text" name="sequence" placeholder="Enter number sequence" required>
        <br>
        <button type="submit">Submit</button>
    </form>
</body>
</html>
"""

# Mapping for Pin 5
pin_5_map = {
    "0": "1",
    "1": "2",
    "10": "3",
    "11": "4",
    "100": "7",
    "101": "8",
    "110": "9",
    "111": "END"
}

# Mapping for Pin 4
pin_4_map = {
    "0": "NEXT",
    "1": "SLEEP",
    "10": "POWER",
    "11": "BLUETOOTH",
    "100": "F10",
    "101": "0",
    "110": "PRINT SCREEN",
    "111": "F5"
}

def set_gpio(pins, enable_pin):
    # Prepare command to send to GPIO server
    command = {
        "enable_pins": {
            "pin5": enable_pin == 5,
            "pin4": enable_pin == 4
        },
        "input_pins": pins
    }
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(('localhost', 65432))
    client_socket.send(str(command).encode())
    client_socket.close()
    print(f"Sent command: {command}")

def process_sequence(sequence):
    print(f"Processing sequence: {sequence}")
    for char in sequence:
        print(f"Processing character: {char}")
        if char in pin_5_map:
            binary_input = format(int(char), '03b')
            print(f"Setting GPIO for Pin 5 with input: {binary_input}")
            set_gpio(binary_input, 5)
            time.sleep(1)
        elif char in pin_4_map:
            binary_input = format(int(char), '03b')
            print(f"Setting GPIO for Pin 4 with input: {binary_input}")
            set_gpio(binary_input, 4)
            time.sleep(1)

@app.route('/')
def index():
    print("Rendering index page")
    return render_template_string(html_template)

@app.route('/control', methods=['POST'])
def control():
    sequence = request.form['sequence']
    print(f"Received sequence: {sequence}")
    process_sequence(sequence)
    return render_template_string(html_template)

if __name__ == '__main__':
    print("Starting GPIO Control Server")
    app.run(host='0.0.0.0', port=5000)
