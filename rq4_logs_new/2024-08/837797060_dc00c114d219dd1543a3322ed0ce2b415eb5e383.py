import os
import tkinter as tk
from tkinter import filedialog, messagebox
from pdf2docx import Converter

class PDFToWordConverter:
    def __init__(self, master):
        self.master = master
        master.title("PDF to Word Converter")
        master.geometry("400x200")

        # Create and pack widgets
        self.label = tk.Label(master, text="Select a PDF file to convert:")
        self.label.pack(pady=10)

        self.select_button = tk.Button(master, text="Select PDF", command=self.select_pdf)
        self.select_button.pack(pady=10)

        self.convert_button = tk.Button(master, text="Convert to Word", command=self.convert_pdf_to_word, state=tk.DISABLED)
        self.convert_button.pack(pady=10)

        self.result_label = tk.Label(master, text="")
        self.result_label.pack(pady=10)

        self.pdf_path = None

    def select_pdf(self):
        self.pdf_path = filedialog.askopenfilename(filetypes=[("PDF Files", "*.pdf")])
        if self.pdf_path:
            self.label.config(text=f"Selected: {os.path.basename(self.pdf_path)}")
            self.convert_button.config(state=tk.NORMAL)
        else:
            self.label.config(text="Select a PDF file to convert:")
            self.convert_button.config(state=tk.DISABLED)

    def convert_pdf_to_word(self):
        if not self.pdf_path:
            messagebox.showerror("Error", "No PDF file selected.")
            return

        # Check if the file is a PDF
        if not self.pdf_path.lower().endswith('.pdf'):
            messagebox.showerror("Error", "The selected file is not a PDF.")
            return

        # Define the output path
        word_path = self.pdf_path.replace('.pdf', '.docx')

        try:
            # Convert PDF to Word
            cv = Converter(self.pdf_path)
            cv.convert(word_path, start=0, end=None)
            cv.close()

            messagebox.showinfo("Success", f"Conversion complete!\nThe file has been saved as:\n{word_path}")
            self.result_label.config(text=f"Converted: {os.path.basename(word_path)}")
        except Exception as e:
            messagebox.showerror("Error", f"An error occurred during conversion:\n{str(e)}")

def main():
    root = tk.Tk()
    app = PDFToWordConverter(root)
    root.mainloop()

if __name__ == "__main__":
    main()