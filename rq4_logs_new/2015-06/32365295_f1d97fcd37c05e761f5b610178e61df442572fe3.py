#!/usr/bin/python
# coding:utf-8
#from xml.etree.ElementTree import ElementTree
#import xml.etree.ElementTree as xml
import xml.etree.ElementTree as xml
# as ET #sql = ET
class XMLResponser:

	def getHosts():
		root = ET.Element('p2pse')
		gethosts = ET.SubElement(root,'getHosts')

		arq = ET.ElementTree(root)
		ET.dump(root)
		arq.write('getHosts.xml')

	def getHostsResponse(ip,port):
		root = ET.Element('p2pse')
		gethostsresponse = ET.SubElement(root,'getHostsResponse')
		host = ET.SubElement(gethostsResponse,'host')
		ip2 = ET.SubElement(host,'ip')
		port2 = ET.SubElement(host,'port')

		#fazer um for aqui  !!!!!!!!!PENSAR!!!!!!!!!!
		ip2.text = ip
		port2 = port

		arq = ET.ElementTree(root)
		ET.dump(root)
		arq.write('getHostsResponse.xml')

	def searchFiles(keywords):
		root = ET.Element('p2pse')
		searchfiles = ET.SubElement(root,'searchFiles')
		keywords2 = ET.SubElement(root,'keywords')

		keywords2.text = keywords

		arq = ET.ElementTree(root)
		ET.dump(root)
		arq.write('searchFiles.xml')

	def searchFilesResponse(file, fileName, fileSize):
		root = ET.Element('p2pse')
		searchfileresponse = ET.SubElement(root,'searchFilesResponse')
		file2 = ET.SubElement(gethostsResponse,'file')
		fileName2 = ET.SubElement(file2,'fileName')
		fileSize2 = ET.SubElement(file2,'fileSize')

		file2.text = file
		fileName2.text = fileName
		fileSize2.text = fileSize

		arq = ET.ElementTree(root)
		ET.dump(root)
		arq.write('getHostsResponse.xml')

	def getFiles(fileName):
		root = ET.Element('p2pse')
		getFiles = ET.SubElement(root,'getFiles')
		fileName2 = ET.SubElement(getFiles, 'fileName')

		fileName2.text = fileName

		arq = ET.ElementTree(root)
		ET.dump(root)
		arq.write('getFiles.xml')

	def getFilesResponse(fileData, fileName, data):
		root = ET.Element('p2pse')
		getFilesResponse = ET.SubElement(root,'getFilesResponse')
		fileData2 = ET.SubElement(getFilesResponse,'fileData')
		fileName2 = ET.SubElement(fileData2,'fileName')
		data2 = ET.Element(fileData2,'data')

		fileData2.text = fileData
		fileName2.text = fileName
		data2.text = data

		arq = ET.ElementTree(root)
		ET.dump(root)
		arq.write('getHostsResponse.xml')
	def sendHosts(ip,port):
		codigo = "<?xml version="+ 1.0+ " encoding="+ "UTF-8"+ " ?><!DOCTYPE p2pse SYSTEM "+ p2pse.dtd+ "><p2pse><getHostsResponse><host><ip>"+ip+"</ip><port>"+port+"</port></host><host><ip>"+ip+"</ip><port>"+port+"</port></host></getHostsResponse></p2pse>"

	'''
	#escrita
	root = ET.Element('pycursos') #no pai
	gtk = ET.SubElement(root, 'pyGTK') #n filho 
	django = ET.SubElement(gtk, 'Django') #no filho, do filho
	django.text = 'Django matando o PHP!' 
	arq = ET.ElementTree(root)

	ET.dump(root) #constroi o arquivo

	#arq.write('pycursos_final.xml')

	#leitura
	doc = ET.parse('pycursos.xml')

	root = doc.getroot() # recupera a tag principal

	print root.tag, root.attrib['nome'] # dicionario

	#listar a galera
	for i in root:
		print i.tag, i.attrib['nome']
		
	for i in root.iter('cidade'):
		print i.tag, i.attrib['nome']
	'''