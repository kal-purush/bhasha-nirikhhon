import xml.etree.ElementTree as ET
tree = ET.parse('Test.ckl')
root = tree.getroot()

Host = input("What is the hostname of the device?")
IP = input("What is the ip address of the device?")
New_File = input("What would you like the new file to be named?")

# Set all STATUS elements to 'NotAFinding'
#for status in root.findall(".//STATUS"):
#    status.text = 'NotAFinding'

host_name = root.find(".//HOST_NAME")
if host_name is not None:
    host_name.text = Host

host_ip = root.find(".//HOST_IP")
if host_ip is not None:
    host_ip.text = IP

# Save the updated XML file
tree.write(New_File, encoding='utf-8', xml_declaration=True)


#FileIOSXESWITCHL2 = input("What is the name of the IOSXE L2 ckl file you are editing?")


V_220649_Open = {
    "file_name": New_File,
    "key": "V-220649",
    "status": "Open",
    "finding_details": " ",
    "comments": " "
}

V_220649_NotAFinding = {
    "file_name": New_File,
    "key": "V-220649",
    "status": "NotAFinding",
    "finding_details": " ",
    "comments": " "
}

V_220650_Open = {
    "file_name": New_File,
    "key": "V-220650",
    "status": "Open",
    "finding_details": " ",
    "comments": " "
}

V_220650_NotAFinding = {
    "file_name": New_File,
    "key": "V-220650",
    "status": "NotAFinding",
    "finding_details": " ",
    "comments": " "
}

V_220651_Open = {
    "file_name": New_File,
    "key": "V-220651",
    "status": "Open",
    "finding_details": " ",
    "comments": " "
}

V_220651_NotAFinding = {
    "file_name": New_File,
    "key": "V-220651",
    "status": "NotAFinding",
    "finding_details": " ",
    "comments": " "
}

V_220655_Open = {
    "file_name": New_File,
    "key": "V-220655",
    "status": "Open",
    "finding_details": " ",
    "comments": " "
}

V_220655_NotAFinding = {
    "file_name": New_File,
    "key": "V-220655",
    "status": "NotAFinding",
    "finding_details": " ",
    "comments": " "
}

V_220656_Open = {
    "file_name": New_File,
    "key": "V-220656",
    "status": "Open",
    "finding_details": " ",
    "comments": " "
}

V_220656_NotAFinding = {
    "file_name": New_File,
    "key": "V-220656",
    "status": "NotAFinding",
    "finding_details": " ",
    "comments": " "
}

V_220657_Open = {
    "file_name": New_File,
    "key": "V-220657",
    "status": "Open",
    "finding_details": " ",
    "comments": " "
}

V_220657_NotAFinding = {
    "file_name": New_File,
    "key": "V-220657",
    "status": "NotAFinding",
    "finding_details": " ",
    "comments": " "
}

V_220658_Open = {
    "file_name": New_File,
    "key": "V-220658",
    "status": "Open",
    "finding_details": " ",
    "comments": " "
}

V_220658_NotAFinding = {
    "file_name": New_File,
    "key": "V-220658",
    "status": "NotAFinding",
    "finding_details": " ",
    "comments": " "
}

V_220659_Open = {
    "file_name": New_File,
    "key": "V-220659",
    "status": "Open",
    "finding_details": " ",
    "comments": " "
}

V_220659_NotAFinding = {
    "file_name": New_File,
    "key": "V-220659",
    "status": "NotAFinding",
    "finding_details": " ",
    "comments": " "
}

V_220660_Open = {
    "file_name": New_File,
    "key": "V-220660",
    "status": "Open",
    "finding_details": " ",
    "comments": " "
}

V_220660_NotAFinding = {
    "file_name": New_File,
    "key": "V-220660",
    "status": "NotAFinding",
    "finding_details": " ",
    "comments": " "
}

V_220661_Open = {
    "file_name": New_File,
    "key": "V-220661",
    "status": "Open",
    "finding_details": " ",
    "comments": " "
}

V_220661_NotAFinding = {
    "file_name": New_File,
    "key": "V-220661",
    "status": "NotAFinding",
    "finding_details": " ",
    "comments": " "
}

V_220662_Open = {
    "file_name": New_File,
    "key": "V-220662",
    "status": "Open",
    "finding_details": " ",
    "comments": " "
}

V_220662_NotAFinding = {
    "file_name": New_File,
    "key": "V-220662",
    "status": "NotAFinding",
    "finding_details": " ",
    "comments": " "
}

V_220663_Open = {
    "file_name": New_File,
    "key": "V-220663",
    "status": "Open",
    "finding_details": " ",
    "comments": " "
}

V_220663_NotAFinding = {
    "file_name": New_File,
    "key": "V-220663",
    "status": "NotAFinding",
    "finding_details": " ",
    "comments": " "
}

V_220664_Open = {
    "file_name": New_File,
    "key": "V-220664",
    "status": "Open",
    "finding_details": " ",
    "comments": " "
}

V_220664_NotAFinding = {
    "file_name": New_File,
    "key": "V-220664",
    "status": "NotAFinding",
    "finding_details": " ",
    "comments": " "
}

V_220665_Open = {
    "file_name": New_File,
    "key": "V-220665",
    "status": "Open",
    "finding_details": " ",
    "comments": " "
}

V_220665_NotAFinding = {
    "file_name": New_File,
    "key": "V-220665",
    "status": "NotAFinding",
    "finding_details": " ",
    "comments": " "
}

V_220666_Open = {
    "file_name": New_File,
    "key": "V-220666",
    "status": "Open",
    "finding_details": " ",
    "comments": " "
}

V_220666_NotAFinding = {
    "file_name": New_File,
    "key": "V-220666",
    "status": "NotAFinding",
    "finding_details": " ",
    "comments": " "
}

V_220667_Open = {
    "file_name": New_File,
    "key": "V-220667",
    "status": "Open",
    "finding_details": " ",
    "comments": " "
}

V_220667_NotAFinding = {
    "file_name": New_File,
    "key": "V-220667",
    "status": "NotAFinding",
    "finding_details": " ",
    "comments": " "
}

V_220668_Open = {
    "file_name": New_File,
    "key": "V-220668",
    "status": "Open",
    "finding_details": " ",
    "comments": " "
}

V_220668_NotAFinding = {
    "file_name": New_File,
    "key": "V-220668",
    "status": "NotAFinding",
    "finding_details": " ",
    "comments": " "
}

V_220669_Open = {
    "file_name": New_File,
    "key": "V-220669",
    "status": "Open",
    "finding_details": " ",
    "comments": " "
}

V_220669_NotAFinding = {
    "file_name": New_File,
    "key": "V-220669",
    "status": "NotAFinding",
    "finding_details": " ",
    "comments": " "
}

V_220670_Open = {
    "file_name": New_File,
    "key": "V-220670",
    "status": "Open",
    "finding_details": " ",
    "comments": " "
}

V_220670_NotAFinding = {
    "file_name": New_File,
    "key": "V-220670",
    "status": "NotAFinding",
    "finding_details": " ",
    "comments": " "
}

V_220671_Open = {
    "file_name": New_File,
    "key": "V-220671",
    "status": "Open",
    "finding_details": " ",
    "comments": " "
}

V_220671_NotAFinding = {
    "file_name": New_File,
    "key": "V-220671",
    "status": "NotAFinding",
    "finding_details": " ",
    "comments": " "
}

V_220672_Open = {
    "file_name": New_File,
    "key": "V-220672",
    "status": "Open",
    "finding_details": " ",
    "comments": " "
}

V_220672_NotAFinding = {
    "file_name": New_File,
    "key": "V-220672",
    "status": "NotAFinding",
    "finding_details": " ",
    "comments": " "
}

V_220673_Open = {
    "file_name": New_File,
    "key": "V-220673",
    "status": "Open",
    "finding_details": " ",
    "comments": " "
}

V_220673_NotAFinding = {
    "file_name": New_File,
    "key": "V-220673",
    "status": "NotAFinding",
    "finding_details": " ",
    "comments": " "
}