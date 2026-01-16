from PyPDF2 import PdfReader
from reportlab.pdfgen import canvas

from utils.path import path_to_lithologies_generated


def lithology_rect(pdf_file_path: str):
    reader: PdfReader = PdfReader(pdf_file_path)
    page = reader.pages[0]
    filename = path_to_lithologies_generated(pdf_file_path)
    pdf_canvas = canvas.Canvas(filename=filename, pagesize=(page.mediabox.width, page.mediabox.height))

    lithologies = get_lithologies()

    add_lithologies_rect_to_pdf(lithologies, pdf_canvas)
    pdf_canvas.save()


def add_lithologies_rect_to_pdf(lithologies, pdf_canvas):
    for lithology in lithologies:
        _, move_x, move_y = lithology[0].split()
        move_x = float(move_x)
        move_y = float(move_y)
        for instruction_a in lithology[1:]:
            name, x, y = instruction_a.split(" ")
            x = float(x)
            y = float(y)
            pdf_canvas.line(move_x, move_y, x, y)
            move_x = x
            move_y = y


def get_lithologies():
    x1, y1 = [0, 0]
    lithologies = []
    rect = []
    with open('teste.ps', "a+") as file:
        file.seek(0)
        lines = file.readlines()

        if len(lines) > 0:
            for i, line in enumerate(lines):
                instruction, x2, y2 = line.split(" ")
                x2 = float(x2)
                y2 = float(y2)
                if instruction == 'm':
                    rect.append(line.strip())
                    if len(lines) > i + 5:
                        for j in range(i + 1, i + 5):
                            instruction, *_ = lines[j].split(" ")
                            if instruction == 'l':
                                rect.append(lines[j].strip())
                            else:
                                break

                        if len(rect) == 5:
                            _, last_line_x, last_line_y = rect[4].split(" ")
                            _, first_move_x, first_move_y = rect[0].split(" ")
                            if last_line_x == first_move_x and last_line_y == first_move_y:
                                lithologies.append(rect[:])
                rect.clear()

        return lithologies


