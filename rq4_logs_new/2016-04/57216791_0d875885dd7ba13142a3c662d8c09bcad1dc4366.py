import xml.etree.ElementTree as etree
from xml.etree.ElementTree import Element, SubElement, Comment

BASE_FILE = 'baseTestCase.xml'
SOURCE_FILE = 'test_data.txt'
TAG_NAME = 'testcase'
OUTFILE = 'output.xml'
STEPS_FILE = 'create_new_user.xml'
STEPS_SOURCE = 'test_data_steps.txt'
TAB = '\t'
LINE_JUMP = '\n'
OUTPUT_FILE = 'testcases.xml'

def is_line_jump(element):
    if element == LINE_JUMP:
        return True
    else: 
        return False

def is_tab(element):
    if element == TAB:
        return True
    else: 
        return False

def clean_steps(steps):
    step = steps.split(TAB)
    step = step[1:3]
    expected_result_size = len(step[1])
    expected_result = step[1] 
    epected_result = expected_result[0:expected_result_size - 1] 
    step[1] = epected_result
    return step

#
#  It creates a small xml to load test cases on testLink only with title
#
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
    tree.write(OUTFILE)

#create_xml()


#
#  It creates a small xml to load test cases on testLink with title and steps 
#
def create_xml_with_steps():
    tree = etree.parse(STEPS_FILE)
    cases = tree.getroot()
    data = open(STEPS_SOURCE)
    data_hash = data.readlines()
    source_size = len(data_hash) 
    index = 0
    title = {}
    step_number_it = 0
    top = Element('testcase')
    first_step = False

    for line in data_hash:
        if index != source_size-1:
            if index == 0:
                cases.clear()
                testcase = Element(TAG_NAME, name=data_hash[index])
                first_step = True
            else: 
                if is_line_jump(line):
                    cases.append(testcase)
                    testcase = Element(TAG_NAME, name=data_hash[index+1])
                    step_number_it = 0
                    first_step = True
                if is_tab(line[0]):
                    if first_step:
                        first_step = False
                        steps = SubElement(testcase, 'steps')
                    step_number_it = step_number_it + 1
                    step_hash = clean_steps(line)
                    step = SubElement(steps, 'step')
                    step_number = SubElement (step, 'step_number')
                    step_number.text = str(step_number_it)
                    actions = SubElement (step, 'actions')
                    actions.text = str(step_hash[0])
                    expectedresults = SubElement (step, 'expectedresults')
                    expectedresults.text = str(step_hash[1])
        else:
            cases.append(testcase)
        index = index + 1
    tree.write(OUTPUT_FILE)

create_xml_with_steps()