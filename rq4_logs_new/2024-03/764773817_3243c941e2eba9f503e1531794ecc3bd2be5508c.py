import tkinter as tk
import json
import random

class OctordleUI:
    def __init__(self, master):
        self.master = master
        master.title('Octordle Game')

        file_path = 'five_letter_words.json'

        with open(file_path, 'r') as file:
            self.target_words = json.load(file)

        self.target_words = random.sample(self.target_words, 8)

        # Define target words for testing
        # self.target_words = ["APPLE", "BERRY", "CHERR", "DATES", "ELDER", "FIGGY", "GRAPE", "HONEY"]
        # Ensure all target words are uppercase
        self.target_words = [word.upper() for word in self.target_words]
        # Track the current guess index and which boards are solved
        self.current_guess_index = 0
        self.solved_boards = [False] * len(self.target_words)

        # Set the size of the window
        master.geometry('1280x800')

        # Define colors
        self.colors = {"correct": "#538d4e", "present": "#b59f3b", "absent": "#3a3a3c"}

        # Create frames for each word's guesses and separators
        self.frames = [tk.Frame(master, width=100, height=600) for _ in range(8)]
        for index, frame in enumerate(self.frames):
            frame.grid(row=1, column=index * 2)
            if index < 7:  # Add separators between sections but not after the last one
                separator = tk.Frame(master, width=2, height=600, bg="black")
                separator.grid(row=1, column=index * 2 + 1, sticky="ns")

        # Create labels for guesses in each frame
        self.guess_labels = [[[tk.Label(frame, text=' ', bg="#3a3a3c", fg="white", font=('Helvetica', 16), width=2,
                                        height=1) for _ in range(5)] for _ in range(13)] for frame in self.frames]
        for frame_index, frame_labels in enumerate(self.guess_labels):
            for guess_index, row in enumerate(frame_labels):
                for letter_index, label in enumerate(row):
                    label.grid(row=guess_index, column=letter_index, padx=2, pady=2)

        # Add input field and submit button
        self.input_field = tk.Entry(master, width=10, font=('Helvetica', 16))
        self.input_field.grid(row=0, column=0, columnspan=8)

        self.submit_button = tk.Button(master, text="Submit Guess", command=self.submit_guess, font=('Helvetica', 14))
        self.submit_button.grid(row=0, column=8, columnspan=2)

    def submit_guess(self):
        guess = self.input_field.get().upper()
        # Clear input field after submission
        self.input_field.delete(0, tk.END)

        if len(guess) == 5 and self.current_guess_index < 13:
            for word_index, target_word in enumerate(self.target_words):
                if not self.solved_boards[word_index]:  # Only update if not already solved
                    correct = self.update_guess(word_index, self.current_guess_index, guess, target_word)
                    if correct:
                        self.solved_boards[word_index] = True  # Mark as solved
                        # Fill the remaining labels with the background color to indicate solved
                        for i in range(self.current_guess_index + 1, 13):
                            for j in range(5):
                                self.guess_labels[word_index][i][j].config(bg="#3a3a3c", fg="#3a3a3c")
            self.current_guess_index += 1

    def update_guess(self, word_index, guess_index, guess, target_word):
        correct_guess = True
        for i, char in enumerate(guess):
            if char == target_word[i]:
                correct_color = self.colors["correct"]
            elif char in target_word:
                correct_color = self.colors["present"]
                correct_guess = False  # Part of the word, but wrong position
            else:
                correct_color = self.colors["absent"]
                correct_guess = False  # Not in the word at all
            self.guess_labels[word_index][guess_index][i].config(text=char, bg=correct_color, fg="white")
        return correct_guess and guess == target_word


# Create the Tkinter application
root = tk.Tk()
app = OctordleUI(root)

# Run the application
root.mainloop()