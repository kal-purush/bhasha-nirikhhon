import os
import pdb 
import xml.etree.ElementTree as ET

ET.register_namespace("","http://www.github/cliffe/SecGen/scenario")
ET.register_namespace("","http://www.w3.org/2001/XMLSchema-instance")
ET.register_namespace("","http://www.github/cliffe/SecGen/scenario")

pathM = 'sudo ruby secgen.rb -s scenarios/examples/vulnerability_examples/insecure_web_applications/commando/'
newfile = ("Updated.xml")
tree = ET.parse('custom.xml')
root = tree.getroot()

#!ET

def printAllbyET():
    print('\nAll attributes:')
    for x in tree.findall(".//"):
        print(x.tag,"|--->",x.attrib,x.text)
   

def vuln():
    
    print("\n\n******************Vulnerabilities*****************\n\n")
    for x in root.findall(".//{http://www.github/cliffe/SecGen/scenario}vulnerability"):
        print(x.tag,"--->",x.attrib) 
            
    ch = input('\nEnter Tag you want to display : ')
    for x in root.findall(".//{http://www.github/cliffe/SecGen/scenario}vulnerability[@module_path={}]".format(ch)):
        print(x.tag,"--->",x.attrib) # To display selected Ta
        
        changes = str(input('\nEnter Your changed tag : '))
        x.attrib['module_path'] = f'{changes}'
        
    tree.write(newfile)
    
    
def encoders():
    print("\n\n******************Encoders*****************\n\n")
    
    for x in tree.findall(".//{http://www.github/cliffe/SecGen/scenario}encoder"):
        print(x.tag,"--->",x.attrib)
    
    ch = input('\nEnter Tag you want to display : ')
    for x in root.findall(f'.//encoder[@name="{ch}"]'):
        print(x.tag,"--->",x.attrib) # To display selected Ta
        
        changes = str(input('\nEnter Your changed tag : '))
        x.attrib['name'] = f'{changes}'
        
    tree.write(newfile)

 
def generator():
    print("\n\n******************Generator*****************\n\n")
    
    for x in tree.findall(".//{http://www.github/cliffe/SecGen/scenario}value"):
        print(x.tag,"--->",x.text)
    
    ch = input('\nEnter Tag you want to display : ')
    for x in root.findall(f'.//generator[@type="{ch}"]'):
        print(x.tag,"--->",x.attrib) # To display selected Ta
        
        changes = str(input('\nEnter Your changed tag : '))
        x.attrib['type'] = f'{changes}'
        
    tree.write(newfile)
    
    
def input1():
    print("\n\n******************Input*****************\n\n")
    for x in tree.findall(".//input"):
            print(x.tag,"--->",x.attrib)
    
    ch = input('\nEnter Tag you want to display : ')
    for x in root.findall(f'.//input[@into_datastore="{ch}"]'):
        print(x.tag,"--->",x.attrib) # To display selected Ta
        
        changes = str(input('\nEnter the tag you want to change : '))
        x.attrib['into_datastore'] = f'{changes}'
        
    tree.write(newfile)
        

print("""   
    Enter 1 to Display All TAGS ATTRIBUTES and TEXTS
    Enter 2 for Vulnerabilities
    Enter 3 for Encoders
    Enter 4 for Values
    Enter 5 for Input
    Enter 6 to display all the tree
    """)

x = 1000

while x!=0:
    x = int(input("\nYour Choice  : "))
    if x==1:
        printAllbyET()
    elif x==2:
        vuln()
    elif x==3:
        encoders()
    elif x==4:
        generator()
    elif x==5:
        input1()
    elif x==6:
        ET.dump(tree)
    else:
        print("\nInvalid Choice")
        
# os.write(pathM)