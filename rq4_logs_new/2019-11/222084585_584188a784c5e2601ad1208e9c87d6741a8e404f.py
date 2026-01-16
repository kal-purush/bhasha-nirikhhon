import io
from PIL import Image
import pytesseract
from wand.image import Image as wi

import shutil
import os
import time


def is_crypto(_filename):
	"""
	returns a bool whether the file is a crypto statement or not
	:param _filename:
	:return:
	"""

	pdf = wi(filename=_filename, resolution=100)
	pdf_image = pdf.convert('jpeg')

	image_blobs = []

	for img in pdf_image.sequence:
		img_page = wi(image=img)
		image_blobs.append(img_page.make_blob('jpeg'))

	recognized_text = []

	for imgBlob in image_blobs:
		im = Image.open(io.BytesIO(imgBlob))
		text = pytesseract.image_to_string(im, lang='eng')
		recognized_text.append(text)

	keyword = "crypto"

	for i in recognized_text:
		# print(i)
		if keyword in i:
			print("WE FOUND IT EXITING")
			print("CRYPTO STATEMENT!!!")
			return True

	print("PRAESCIRE STATEMENT")
	return False


def delete_cache():
	tempdir = '/tmp'
	files = os.listdir(tempdir)
	for file in files:
		if "magick" in file:
			os.remove(os.path.join(tempdir, file))
	print("Removed temporary files")


def ocr_classification():
	# statements_path = "/home/joe/PycharmProjects/ezgmail_statements/statements_folder/"
	statements_path = "/home/joe/Dropbox/praescire_statements/"
	files = os.listdir(statements_path)
	# files = [f for f in os.listdir(statements_path) if os.path.isfile(f)]
	print(files)

	newlist = []
	for names in files:
		if names.endswith(".pdf"):
			newlist.append(names)
	print(newlist)

	delete_cache()

	count = 0
	for pdf in newlist:
		base_path = statements_path
		crypto_store = statements_path + "/crypto/"
		pdf_path = base_path + pdf
		print(pdf_path)

		if is_crypto(pdf_path):
			shutil.move(pdf_path, crypto_store + pdf)

		# every 5 files we will clear cache and pause for a minute after 20 files
		if count % 5 == 0:
			print("count is ", count, ": CLEARING CACHE")
			delete_cache()
			if count % 20 == 0:
				time.sleep(60)

		count += 1
	print("completed moving the files around")

	delete_cache()


if __name__ == "__main__":
	ocr_classification()