import xml.etree.ElementTree as etree

BASE_FILE = 'baseTestCase.xml'
SOURCE_FILE = 'test_data.txt'
TAG_NAME = 'testcase'
STEPS_FILE = 'create_new_user.xml'
STEPS_SOURCE = 'test_data_steps.txt'
OUTPUT_FILE = 'testcaseswithoutsteps.xml'

def create_xml():
    tree = etree.parse(BASE_FILE)
    cases = tree.getroot()
    data = open(SOURCE_FILE)
    data_hash = data.readlines()

    result = {}

    index = 0
    for title in data_hash:
        new = etree.Element(TAG_NAME, name=title)
        cases.append(new)

    cases[0].clear()
    tree.write(OUTPUT_FILE)

create_xml()