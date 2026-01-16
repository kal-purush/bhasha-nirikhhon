'''
Created on 29.04.2016

@author: cypher9
'''
import os.path


from xml.etree import ElementTree
from xml.dom import minidom
from xml.etree.ElementTree import Element, SubElement
import xml.etree.ElementTree as ET
from _elementtree import tostring
 

def get_root():
    return ET.parse('termine.xml').getroot()

def write_xml(event):
    xml_root = None
    xml_top = None
    try:
        if os.path.isfile("termine.xml"):
            xml_root = get_root()
            xml_top = ET.SubElement(xml_root, 'event')
        else:
            xml_root = Element('planner')
            xml_top = SubElement(xml_root, 'event')
        
        child = ET.SubElement(xml_top, 'title')
        child.text= event.event_title
    
        child = ET.SubElement(xml_top, 'description')
        child.text= event.event_description.rstrip()
    
        child = ET.SubElement(xml_top, 'startdatetime')
        child.text= str(event.event_start_datetime)
    
        child = ET.SubElement(xml_top, 'enddatetime')
        child.text= str(event.event_end_datetime)
    
    
        xmlfile=open('termine.xml','w')
        l=tostring(xml_root)
        xmlfile.write(l)     
        xmlfile.close
    except:
        print("Error writing file!")


def prettify(self, elem):
    """
    Return a pretty-printed XML string for the Element.
    """
    rough_string = ElementTree.tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")
        
        
    