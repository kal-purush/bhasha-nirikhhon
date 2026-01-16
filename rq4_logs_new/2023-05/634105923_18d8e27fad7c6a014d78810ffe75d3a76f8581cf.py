import tkinter as tk
from tkinter import ttk

class MainWindow(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("oss-project GUI git repository")

        self.geometry("300x200")
        self.resizable(False, False)

        # oss-project label
        oss_label = tk.Label(self, text="oss-project", font=("Helvetica", 8))
        oss_label.place(relx=0.25, rely=0.05, anchor="ne")

        # GUI git repository label
        repo_label = tk.Label(self, text="GUI git repository", font=("Helvetica", 20,"bold"))
        repo_label.place(relx=0.5, rely=0.35, anchor="center")

        # 파일창 열기 button
        open_button = ttk.Button(self, text="start")
        open_button.place(relx=0.5, rely=0.8, anchor="center")

if __name__ == "__main__":
    window = MainWindow()
    window.mainloop()