#!/usr/bin/env python
from __future__ import division
import copy, math, sys, getopt
from PyPDF2 import PdfFileWriter, PdfFileReader
from PyPDF2.pdf import PageObject


def impaginate(inputFile, outputFile):
	file1 = PdfFileReader(file(inputFile, "rb"))
	output = PdfFileWriter()


	# (0,0) in basso a sinistra
	(upperLeftX,upperLeftY) = file1.getPage(0).mediaBox.upperLeft
	(pageWidth, pageHeight) = file1.getPage(0).mediaBox.upperRight
	pageWidth = float(pageWidth)
	pageHeight = float(pageHeight)
	print "Dimensioni", pageWidth, pageHeight
	## Margine con 486x720 e' 25
	#MARGIN_O = 25*486/pageWidth 
	#MARGIN_V = 25*720/pageHeight # e' un magic number
	ratio = pageWidth/pageHeight
	# Magari si puo' rimpocciolire ancora con:
	# ratio = ratio *0.95
	# Ma e' necessario effettuare il centering delle pagine
	#print "Margin:", MARGIN_O, MARGIN_V, "Ratio:", ratio

	numPages = file1.numPages

	scaledHeight = pageWidth*ratio # considero anche la traslazione
	scaledWidth = pageHeight*ratio

	print scaledWidth, scaledHeight, scaledHeight*2
	deltaH = (pageHeight - scaledHeight*2)/2
	deltaW = (0)/2
	
	cntCreated = 0
	for index in range(numPages):
		print "Elaboro pagina:", index+1, "/", numPages
		if index % 2 == 0:
			# imposto la pagina sopra
			pageA = file1.getPage(index)
			if (numPages % 2 == 1 and index == numPages-1):
				#ultima pagina da sola
				page = PageObject.createBlankPage(width=pageWidth, height=pageHeight)
				#page.mergeRotatedScaledTranslatedPage(pageA, 270, ratio, MARGIN_O/2, pageHeight   - MARGIN_V/2)
				page.mergeRotatedScaledTranslatedPage(pageA, 270, ratio, deltaW, pageHeight   - deltaH)
				if cntCreated % 2 == 1:
					# Se la pagina e' dispari la ruoto
					page.rotateClockwise(180)
				cntCreated += 1
				output.addPage(page)
		else:
			# imposto la pagina sotto
			pageB = file1.getPage(index)
			# creo la nuova pagina
			page = PageObject.createBlankPage(width=pageWidth, height=pageHeight)
			#page.mergeRotatedScaledTranslatedPage(pageA, 270, ratio, MARGIN_O/2, pageHeight   - MARGIN_V/2)
			#page.mergeRotatedScaledTranslatedPage(pageB, 270, ratio, MARGIN_O/2, pageHeight/2 - MARGIN_V/2)
			page.mergeRotatedScaledTranslatedPage(pageA, 270, ratio, deltaW, pageHeight   - deltaH)
			page.mergeRotatedScaledTranslatedPage(pageB, 270, ratio, deltaW, pageHeight/2 - deltaH)
			if cntCreated % 2 == 1:
				# Se la pagina e' dispari la ruoto
				page.rotateClockwise(180)
			cntCreated += 1
			output.addPage(page)

	print "Genero file ..."
	outputStream = file(outputFile, "wb")
	output.write(outputStream)
	outputStream.close()



def main(argv):
  	inputFile = ''
  	outputFile = ''
   	try:
		(opts, args) = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])
   	except getopt.GetoptError:
		print "impaginator.py -i <inputfile> -o <outputfile>"
		sys.exit(2)
   	for opt, arg in opts:
		if opt == "-h":
			print "impaginator.py -i <inputfile> -o <outputfile>"
			sys.exit()
		elif opt in ("-i", "--ifile"):
			inputFile = arg
		elif opt in ("-o", "--ofile"):
			outputFile = arg

	if outputFile == "":
		outputFile = "document-output.pdf"
	if inputFile == "":
		print "Non hai specificato un file di input"
		sys.exit()

	impaginate(inputFile, outputFile)


if __name__ == "__main__":
	main(sys.argv[1:])