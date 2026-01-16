'''
   Environment:  Python 2.7.x
  Program Name:  fmdr.py
        Author:  Peter Mo
          Date:  20150430 (YYYYMMDD)
 Windows Usage:  python fmdr.py > fmdr.out
    *nix Usage:  python fmdr.py | fmdr.out

   Description:  This program reads java classes that represent a data model.
                 It strips out the java fields, along with its data type.
                 The output is used to configure the FMDR (OpenFurther
                 Metadata Repository). Be sure to verify the output of this program
                 by comparing the number of java fields to the actual number
                 of data model columns.
          Note:  This initial may not be perfect.
                 The python indentation requirements will drive you nuts if you're not used to it.
                 I am using 2 spaces for indentations.
                 Also note that multi-line comments are 3 single quotes together.

          Issue: Using Java or Groovy may be more difficult because in order to resolve java class 
                 dependencies, it is messy business. Python is much simpler.
'''

# Import Modules Here:
import sys,os,glob
# Import datetime module
from datetime import datetime


# MUST Define Functions BEFORE Usage
def printField(pClassName, pkFlag='N'):
  # print pClassName # DEBUG

  # Needed to modify global counter
  global gCounter

  # Open Each File
  with open(pClassName) as f:
    # Debug f.name File Attribute
    # print 'Inside printField Function ' + f.name

    # Loop through Each Line in the File
    for line in f:

      # Trim off spaces and tabs in the Current Line
      trimmed_line = line.strip()

      # Look for Lines that match these 3 criteria
      if ( trimmed_line.startswith('private') 
           and 'serialVersionUID' not in trimmed_line
           and 'PK id' not in trimmed_line ):

        # Increment Counter
        gCounter += 1

        # Cleanup the Java Field String
        javaField = trimmed_line.split(' ',1)[1].replace(';','')

        # Concat the ClassName to Java Field separated by Space Delimiter
        if pkFlag == 'N':
          result = f.name.replace('.java','') + ' ' + javaField
          # Print to Standard Output
          print str(gCounter) + ' ' + result
        else:
          result = f.name.replace('PK.java','') + ' ' + javaField
          # Print to Standard Output with PK Hint
          print str(gCounter) + '_PK ' + result


# FUNCTION to Process Java Fields
def processField(pClassList):

  # Loop through the Java Files to get Java Field Names
  print '# BEG JAVA FIELDS ' + str(datetime.now()) + ' #'

  # Place all *PK.java files immediately after the base file
  for c in pClassList:
    # Get Java Fields for base Entity Types
    printField(c+'.java')
    
    # look for the matching PK file if exist
    for pkFileName in glob.glob(c+'PK.java'):
      printField(pkFileName,'Y')

  print '# END JAVA FIELDS ' + str(datetime.now()) + ' #'


def processClass():
  # Print File Names, which are same as Class Names
  print '# BEG JAVA CLASSES ' + str(datetime.now()) + ' #'

  # init classList Variable
  classList = []

  # Build classList using filenames ending with .java
  for fileName in glob.glob('*.java'):
    # Skip all files ending with PK.java
    if not fileName.endswith('PK.java'):
      # Get filename without the file extension
      base=os.path.splitext(fileName)[0]
      classList.append(base)

  # Sort the classList
  classList.sort()

  # Print list of base classes
  for index,c in enumerate(classList, start=1):
    print str(index) + ' ' + c

  print '# END JAVA CLASSES ' + str(datetime.now()) + ' #\n'

  # Return Sorted Class List
  return classList


# MAIN FUNCTION
def main():

  # CONFIGURE Java Files Directory Location
  dirPath = 'C:\Users\peter\Documents\UU_BMIC\FURTHeR\JavaObjects\OFCDM_v151'

  # Change Current Working Directory to Java Files
  os.chdir(dirPath)

  # Call processClass() returns classList
  cl = processClass()

  # Call processField(classList)
  processField(cl)




# ########## CALL MAIN HERE! ##########

# Initialize Global Counter for Each Valid Java Field
# Be sure to declare globals outside of main function
gCounter = 0

# Run
main()