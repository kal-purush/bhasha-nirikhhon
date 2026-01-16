#!/usr/bin/env python
import random
import rospy

def cage_height(param):
    if (param == "rand"):
        return random.uniform(args['cage_height_range_begin'], args['cage_height_range_end'])

def updateArgs(arg_defaults):
    '''Look up parameters starting in the driver's private parameter space, but
    also searching outer namespaces.  Defining them in a higher namespace allows
    the axis_ptz.py script to share parameters with the driver.'''
    args = {}
    for name, val in arg_defaults.iteritems():
        full_name = rospy.search_param(name) #search without postfix
        if full_name is None:   #search with postfix
            full_name = rospy.search_param(name)
        if full_name is None:  #use default
            args[name] = val
        else:
            args[name] = rospy.get_param(full_name, val)
    return(args)

nodename = "generating_terrain"
node = rospy.init_node(nodename)

args_default = {
        'cage_height_range_begin': '0.1',
        'cage_height_range_end': '0.6',
        'cage_width_and_lengh': '1',
        'cell_width_number':   '10',
        'cell_length_number':   '10',
        'cage_height_param':   'rand',
        'file_path':   '../maps/Generated_terrain/model.sdf'
      }
args = updateArgs(args_default)

if args['cell_width_number'] % 2 == 1:
    first_point = - ((args['cell_width_number'] - 1) / 2 * args['cage_width_and_lengh'])
else:
    first_point = - (args['cage_width_and_lengh'] / 2 + ((args['cell_width_number'] / 2) * args['cage_width_and_lengh']))

writeFile = open(args['file_path'], 'w')
writeFile.write("<?xml version='1.0'?>\n<sdf version='1.6'>\n	<model name='Terrain'>\n		<static>true</static>")

lis = ["collision", "visual"]
for i in range(args['cell_width_number']):
    for j in range(args['cell_length_number']):
        cur_cage_height = cage_height("rand")
        writeFile.write("\n		<link name=\"box_" + str(i) + "_" + str(j) + "\">\n")
        writeFile.write("			<pose>" + str(first_point + i * args['cage_width_and_lengh']) + " " + str(
            - j * args['cage_width_and_lengh']) + " " + str(cur_cage_height / 2) + " 0 0 0</pose>\n")
        writeFile.write(
            "			<inertial>\n				<mass>1.0</mass>\n				<inertia>\n					<ixx>0.083</ixx>\n 					<ixy>0.0</ixy>\n   					<ixz>0.0</ixz>\n   					<iyy>0.083</iyy>\n 					<iyz>0.0</iyz>\n   					<izz>0.083</izz>\n 				</inertia>\n			</inertial>\n")
        for nam in lis:
            writeFile.write("			<"+nam+" name=\""+nam+"\">\n")
            writeFile.write("				<geometry>\n					<box>\n						")
            writeFile.write("<size>" + str(args['cage_width_and_lengh'])+" "+str(args['cage_width_and_lengh'])+" "+ str(cur_cage_height)+ "</size>\n					</box>\n				</geometry>\n")
            writeFile.write("			</"+nam+">\n")
        writeFile.write("		</link>\n")


writeFile.write("	</model>\n</sdf>")
writeFile.close()

