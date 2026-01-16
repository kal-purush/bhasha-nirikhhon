import PyPDF2
import subprocess

def Extract_text_from_pdf(C:\Users\V JASHWANTH\Downloads\Cheat_sheet_ADB.pdf
    with open("C:\Users\V JASHWANTH\Downloads\Cheat_sheet_ADB.pdf", 'rb') as file:
        reader = PyPDF2.PdfFileReader(file)
        text = ''
        for page_num in range(reader.numPages):
            page = reader.getPage(page_num)
            text += page.extract_text()
        return text

def execute_commands(commands):
    for command in commands.split('\n'):
        if command.strip():  
            print(f"Executing: {command}")
            result = subprocess.run(command, shell=True, capture_output=True, text=True)
            print(result.stdout)
            if result.stderr:
                print(f"Error: {result.stderr}")

pdf_path = 'Cheat_sheet_ADB.pdf'
commands_text = extract_text_from_pdf(pdf_path)
execute_commands(commands_text)