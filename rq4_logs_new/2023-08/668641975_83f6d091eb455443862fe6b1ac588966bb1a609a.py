import json

class Process:

    def __init__(self, file_path) -> None:
        self.file_path = file_path
        self.steps = []

    def load_steps(self):
        with open(self.file_path, 'r', encoding='utf-8') as file:
            self.steps = json.load(file)

    def start(self):
        print(self.steps)