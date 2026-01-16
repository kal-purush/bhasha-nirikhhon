import tkinter as tk
from tkinter import messagebox
import random
import timeit
from typing_test_results import TypingTestResults
from typing_test_app import TypingTestApp


if __name__ == "__main__":
    root = tk.Tk()
    app = TypingTestApp(root)
    root.mainloop()