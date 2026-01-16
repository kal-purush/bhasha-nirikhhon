import easyocr
import fitz
import json
import os
import xlsxwriter


PDFS_PATH = "pdfs"

CURRENT_WORKING_DIRECTORY = os.getcwd()
RESULTS_PATH = os.path.join(CURRENT_WORKING_DIRECTORY, "results")
RESULT_JSON_PATH = os.path.join(RESULTS_PATH, "results.json")
RESULT_EXCEL_PATH = os.path.join(RESULTS_PATH, "results.xlsx")


def list_pdf_files(directory):
    print("Listing pdf files in the directory {}".format(directory))
    pdf_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".pdf"):
                pdf_files.append({"name": file, "path": os.path.join(root, file)})
    return pdf_files


def clean_results_folder():
    for file in os.listdir("results"):
        file_path = os.path.join("results", file)
        if os.path.isfile(file_path):
            os.remove(file_path)


def ensure_results_folder_exists():
    if not os.path.exists(RESULTS_PATH):
        os.makedirs(RESULTS_PATH)


def ocr_file(reader, pdf_file, results_dict):
    doc = fitz.open(pdf_file["path"])
    zoom = 4
    mat = fitz.Matrix(zoom, zoom)
    count = 0

    # Count variable is to get the number of pages in the pdf
    for p in doc:
        count += 1

    for i in range(count):
        val = f"temp/image_{i+1}.png"
        page = doc.load_page(i)
        pix = page.get_pixmap(matrix=mat)
        pix.save(val)
    doc.close()

    with open(os.path.join(RESULTS_PATH, f"{pdf_file['name']}.txt"), "w") as f:
        for i in range(count):
            result = reader.readtext(f"temp/image_{i+1}.png", detail=0)
            f.write(str(result) + "\n")
            if pdf_file["name"] not in results_dict:
                results_dict[pdf_file["name"]] = {}
                results_dict[pdf_file["name"]]["path"] = pdf_file["path"]
                results_dict[pdf_file["name"]]["text"] = " ".join(result)
            else:
                results_dict[pdf_file["name"]]["text"] += " ".join(result)

    for file in os.listdir("temp"):
        file_path = os.path.join("temp", file)
        if os.path.isfile(file_path):
            os.remove(file_path)


def write_to_excel(results_dict):
    with xlsxwriter.Workbook(RESULT_EXCEL_PATH) as workbook:
        worksheet = workbook.add_worksheet("Pdf2Text Results")

        worksheet.write(0, 0, "Název PDF")
        worksheet.write(0, 1, "Cesta")
        worksheet.write(0, 2, "Text")

        row_num = 1
        for key, value in results_dict.items():

            print(f"Writing {key} to excel")
            print(f"Value: {value} of type {type(value)}")

            worksheet.write(row_num, 0, key)
            worksheet.write_url(row_num, 1, value["path"])
            worksheet.write(row_num, 2, value["text"])
            row_num += 1


def main():
    pdf_files = list_pdf_files(PDFS_PATH)
    print(pdf_files)

    reader = easyocr.Reader(["cs"])

    ensure_results_folder_exists()
    clean_results_folder()

    results_dict = {}

    for pdf_file in pdf_files:
        ocr_file(reader, pdf_file, results_dict)

    with open(RESULT_JSON_PATH, "w") as f:
        json.dump(results_dict, f)

    write_to_excel(results_dict)


if __name__ == "__main__":

    main()