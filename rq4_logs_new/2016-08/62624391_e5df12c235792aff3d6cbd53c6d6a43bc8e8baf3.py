"""
                ------------------
                MODULE DESCRIPTION
                ------------------
Current module was developed to check the error frames based on the following
conditions.

    1) In Each frame the protocol presence is mandatory and absence of the
    protocol is treated as an error frame.

    2) Each frame is not restricted to a single line and it can be in multiple
    lines.

Solution:

    1) Check the error frame i.e NoRANAP and store the frame number, position
    in which the error frame is present.

    2) Check the above and below lines for the same frame number and also
    for the presence of the protocol.

    3) If found its not an error.


Sample Log:

Dec Frame # 1	SCTP	M3UA	SCCP	NoBSSAP	NoRANAP	NoMMCC	NoGMM	NoTCAP	NoMAP

Dec Frame # 1	SCTP	M3UA	SCCP	NoBSSAP	RANAP	NoMMCC	NoGMM	NoTCAP	NoMAP

Dec Frame # 1	SCTP	M3UA	SCCP	NoBSSAP	RANAP	NoMMCC	NoGMM	NoTCAP	NoMAP

Dec Frame # 1	SCTP	M3UA	SCCP	NoBSSAP	RANAP	NoMMCC	NoGMM	NoTCAP	NoMAP

Dec Frame # 1	SCTP	M3UA	SCCP	NoBSSAP	RANAP	MMCC	NoGMM	NoTCAP	MAP

Dec Frame # 2	SCTP	M3UA	SCCP	NoBSSAP	RANAP	MMCC	NoGMM	NoTCAP	MAP

Dec Frame # 3	SCTP	M3UA	SCCP	NoBSSAP	RANAP	MMCC	NoGMM	NoTCAP	MAP

Dec Frame # 4	SCTP	M3UA	SCCP	NoBSSAP	RANAP	MMCC	NoGMM	NoTCAP	MAP

Dec Frame # 4	SCTP	M3UA	SCCP	NoBSSAP	RANAP	NoMMCC	NoGMM	NoTCAP	NoMAP

Dec Frame # 4	SCTP	M3UA	SCCP	NoBSSAP	RANAP	MMCC	NoGMM	NoTCAP	NoMAP

Dec Frame # 5	SCTP	M3UA	SCCP	NoBSSAP	RANAP	MMCC	NoGMM	NoTCAP	MAP

Dec Frame # 6	SCTP	M3UA	SCCP	NoBSSAP	RANAP	MMCC	NoGMM	NoTCAP	MAP

Dec Frame # 7	SCTP	M3UA	SCCP	NoBSSAP	RANAP	MMCC	NoGMM	NoTCAP	NoMAP

Dec Frame # 7	SCTP	M3UA	SCCP	NoBSSAP	RANAP	MMCC	NoGMM	NoTCAP	MAP

Dec Frame # 8	SCTP	M3UA	SCCP	NoBSSAP	RANAP	MMCC	NoGMM	NoTCAP	MAP

Dec Frame # 9	SCTP	M3UA	SCCP	NoBSSAP	RANAP	MMCC	NoGMM	NoTCAP	MAP

Dec Frame # 10	SCTP	M3UA	SCCP	NoBSSAP	RANAP	MMCC	NoGMM	NoTCAP	MAP

"""

import csv
import sys

file_path = "PATH OF THE FILE"

proto_dict = [ {"name" : "M3UA", "line" : "2"},
               {"name" : "SCCP", "line" : "3"},
               {"name" : "BSSAP", "line" : "4"},
               {"name" : "RANAP", "line" : "5"},
               {"name" : "MMCC", "line" : "6"},
               {"name" : "GMM", "line" : "7"},
               {"name" : "TCAP", "line" : "8"},
               {"name" : "MAP", "line" : "9"}
              ]
display = 0 
column = 0
contents = []
frame_no = None
pos = None
result = []
error_frames = []
protocol_check = []
LINE_COUNT = 10

""" error printing function """
def usage():
    print("ENTERED wrong protocol, use these : ")
    for name in range(len(proto_dict)):
       print(proto_dict[name]["name"])
    exit()

""" error checking conditions """
for proto in range(len(proto_dict)):
     
    try : 
        if (not (sys.argv[1].upper() in proto_dict[proto]["name"])):
            display += 1
            
        if (sys.argv[1].upper() == proto_dict[proto]["name"]):
            column = proto_dict[proto]["line"]
            
    except IndexError:
        usage()
    
    if (display == len(proto_dict)):    
        usage()


with open(file_path, 'r') as csvfile:
    """    splits each line of the file based on delimiter  """
    lines = csv.reader(csvfile, delimiter = "\t")

    """ Each line is added to the list : frames """    
    for line in lines:
        if len(line) != 0:
            contents.append(line)

for line in range(len(contents)):

    """checks for missing protocol, eg: NoRANAP"""
    
    if( contents[line][int(column)] != sys.argv[1].upper()):

        """ split the line using the delimiter, RHS will result in frame no """
        get_frame_no = contents[line][0].split("#")
        pos = line
        frame_no = get_frame_no[1]
        single_frame = []
    
        """
        from the above code, position/line at which the NoRANAP and frame Num
        is found. Now check the condition if it matches for the error condition.
        i.e RANAP should present in same frame Num which can be either in the
        above lines or below lines with respect to frame_no.
        """
        
        for check in range(pos-LINE_COUNT, pos+LINE_COUNT, 1):

            """
            condition to check if the operation pos-1 should not result in a
            negative num. This can occur in case of NoRANAP found in the first
            line i.e zero position
            """
            try :         
              if (check >= 0):
                
                check_frame = contents[check][0].split("#")

                """
                check if the required frame Num matches, if matched then check
                for RANAP presence.
                """
                if check_frame[1] == frame_no:
                    
                    protocol_check.append(contents[check][int(column)])
            except IndexError:
                pass
            
        if(not(sys.argv[1].upper() in protocol_check)):
            error_frames.append(frame_no)
            
        protocol_check = []

                
result = set(error_frames)

if result:
    for error in result:
        print("ERROR IN FRAME NO : ", error)

else:
    print("NO ERROR FRAMES FOUND")

    