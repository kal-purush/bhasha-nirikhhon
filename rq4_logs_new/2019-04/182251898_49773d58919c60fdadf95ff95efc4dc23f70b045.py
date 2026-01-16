#!/usr/bin/env python3.7

# In each picture file replace specified color with transparency.
# This application requires python version 3.7 or higher, because the argument 'capture_output' of the method 'subprocess.run' was added in python 3.7 .

from __future__ import print_function
import argparse
import os
import subprocess
import sys

def parse_arguments():
    d="""In each picture file replace specified color with transparency.
This application is for batch processing of picture files.
This application passes picture files to application 'convert' in the software suit 'ImageMagick'.
Software ImageMagick is required to be installed.
This application requires python version 3.7 or higher to be installed, because the argument 'capture_output' of the method 'subprocess.run' was added in python 3.7 .
Example of use on linux command line: 
python3.7 replace_color_in_image_with_transparency.py "path/to/image/file/with/color/to/be/replaced.jpg";
Example of use on linux command line: 
python3.7 replace_color_in_image_with_transparency.py --color "rgb(0,0,0)" --filename-add "_transparent_instead_of_black" --output-dir "path/to/output/directory" "path/to/image/file/with/color/to/be/replaced.jpg";
Example of use on linux command line: 
ls directory/with/images/* | xargs python3.7 replace_color_in_image_with_transparency.py --color "rgb(0,0,0)" --filename-add "_transparent_instead_of_black" --output-dir "path/to/output/directory";
"""
    parser=argparse.ArgumentParser(description=d)
    parser.add_argument('--color',dest='color',type=str,default='rgb(0,0,0)',help="""
Color as rgb triplet e.g. 'rgb(255,0,100)' that will be replaced in the output file with transparency.""")
    parser.add_argument('--filename-add',dest='filename_addition',type=str,default='',help="""
Add string to output filename, 
default is "_transparent", 
e.g. when run with --filename-add "_transparent_background" on input file "picture_filename.jpg", 
this will result in output filename "picture_filename_transparent_background.jpg" .""")
    parser.add_argument('--output-dir',dest='output_directory',type=str,default='',help="""
Path to directory where output files will be created, 
by default this path will be the same as the directory that the input file is from.""")
    parser.add_argument('input_path',type=str,nargs='+',help="""
Path to the input image file to be processed, there can be one or more arguments.""")
    args=parser.parse_args()
    return args

def exit_with_error(exit_code,message):
    m="""### ERROR!!! ###
### Error message:
"""+message+"""
###
### Error encountered. Aborting. ###
"""
    print(m)
    sys.exit(exit_code)

def check_python_version():
    if sys.version_info[0]<3 or (sys.version_info[0]==3 and sys.version_info[1]<7):
        m="""This application requires python version 3.7 or higher, because the argument 'capture_output' of the method 'subprocess.run' was added in python 3.7 . 
The version of python that you used to run this application is '"""+sys.version+"'"
        exit_with_error(1,m)

def convert(args):
    # construct and check output file path:
    output_directory=""
    if args.output_directory!='':
        output_directory=os.path.abspath(args.output_directory)
        if not os.path.exists(output_directory):
            exit_with_error(1,"The given output directory '"+args.output_directory+"' does not exist!!!")
        if not os.path.isdir(output_directory):
            exit_with_error(1,"The given output directory '"+args.output_directory+"' is not a directory!!!")
    # main loop
    i=0
    n=1
    number_of_input_files=len(args.input_path)
    while i<number_of_input_files:
        # construct input file path:
        input_path=os.path.abspath(args.input_path[i])
        # construct output file path:
        filename_and_extension=os.path.splitext(os.path.basename(input_path))
        if output_directory=="":
            output_directory=os.path.dirname(input_path)
        output_path=os.path.join(output_directory,filename_and_extension[0]+args.filename_addition+filename_and_extension[1])
        print("---")
        print("   item # "+str(n)+"   ..   "+input_path+"   ->   "+output_path)
        # check input file path:
        if not os.path.exists(input_path):
            exit_with_error(1,"The input path '"+input_path+"' does not exist!!!")
        if not os.path.isfile(input_path):
            exit_with_error(1,"The input path '"+input_path+"' is not a regular file!!!")
        # check output file path:
        if os.path.exists(output_path):
            exit_with_error(1,"The output file \n'"+output_path+"'\n already exists!!!")
        # run the command 'convert':
        process=subprocess.run(["convert",input_path,"-transparent",args.color,output_path],capture_output=True)
        # exit with error message if the command 'convert' encountered an error:
        if process.returncode==0:
            if process.stdout:
                print(process.stdout)
            print("*** success !, output file created ! ***")
        else:
            exit_with_error(1,"The application 'convert' terminated with exit code '"+process.returncode+"' and with error message \n"+str(process.stderr))
        n+=1
        i+=1
    return i

def main():
    print("### Starting... ###")
    print("### Warning: This application requires python version 3.7 or higher, because the argument 'capture_output' of the method 'subprocess.run' was added in python 3.7 . ###")
    args=parse_arguments()
    check_python_version()
    n=convert(args)
    print("### Finished ###")
    print("### Total number of input image files processed: "+str(n)+" ###")
    print("### Total number of image files created: "+str(n)+" ###")
    print("### Successfully finished without an error. Exiting. ###")

if __name__ == "__main__":
    main()