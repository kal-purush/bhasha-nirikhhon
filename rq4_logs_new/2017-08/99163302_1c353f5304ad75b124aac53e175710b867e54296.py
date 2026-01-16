import numpy as np
import csv
from sigqc import sigqc_primitives

##############################################################################
# sigqc_asciitestcase.py
# Austin Coleman
#
# This module encompasses four classes, SigQCAsciiHeader, SigQCAsciiMatrix,
# SigQCAsciiLimits, and SigQCAsciiTestCaseFile which are needed to
# parse and encapsulate all the information contained within a SigQC
# ASCII Test Case File exported from SigQC. Its usage should be mainly
# internal. This submodule relies on the sigqc_primitives submodule from
# the sigqc library.
#
# Example Usage:
#   fpath = "ascii test case file path\\"
#   fname = "ascii test case file name"
#   ascii_object = SigQCAsciiTestCaseFile(fpath+fname)
#   headers = ascii_object.getHeaders()
#
#   ### Many access methods for metadata from SigQCAsciiHeader objects ###
#   for header in headers:
#       product = header.getProdName()  
#       print(product)
#       test = header.getTestName()
#       print(test)
#
#   ### Handle test cases individually ###
#   first_matrix = ascii_object.getMatrixAt(0)
#   serial_numbers = first_matrix.getSerialNumbers()
#   domains = first_matrix.getXValues()
#   data = first_matrix.getYValues()
#
#   ### Handle all test cases at once ###
#   ### Only useful when all serial numbers are consistent ###
#   serialnumbers, domains, data, limits = ascii_object.getAllTestCases()
#
##############################################################################


###################################
# SigQCAsciiHeader class
###################################
class SigQCAsciiHeader:
    '''
    The SigQCAsciiHeader class is designed to encapsulate all of the information for a
    single SigQC test case data section enclosed within a BEGINHEADER and ENDHEADER
    section of the file.  The header information manages a test case identifier, number
    of samples, and measurement units.
    '''
    def __init__(self, i_reader=None):
        '''
        Constructor for an instance of the SigQCAsciiHeader class used to optionally
        read the contents of the file at the first line after a BEGINHEADER.
        
        Input:
            i_reader -  Optionally specify a CSV reader that will be used to fill
                        the contents of the header on construction.  The reader will
                        be used to read up to the point where the next ENDHEADER is
                        encountered.
        '''
        self._line = -1
        self._testcase = None
        self._datasource = None
        self._functiontype = None
        self._elements = 0
        self._xunits = None
        self._yunits = None
        if (i_reader != None):
            self.read(i_reader)
            
    def __str__(self):
        result = ""
        if (not self._testcase):
            result = "<No Test Case>"
        else:
            prodname = self._testcase.GetProductName()
            testname = self._testcase.GetTestName()
            casename = self._testcase.GetCaseName()
            samples = self._elements
            xunits = self._xunits
            yunits = self._yunits
            result = "{}.{}.{}: Samples={}: X-Units={}: Y-Units={}".format(prodname, testname, casename, samples, xunits, yunits)
        return result
     
    def read(self, i_reader):
        '''
        Read the content of a header section within a SigQC ASCII text export file using the
        CSV file reader given.  This method assumes the reader has reached a BEGINHEADER line
        and the next line to be read is first line of the header section.
        '''
        self._line = i_reader.line_num
        
        # Read the product, acceptance test and test case names...
        prodname = ''.join(next(i_reader))
        testname = ''.join(next(i_reader))
        casename = ''.join(next(i_reader))
        self._testcase = sigqc_primitives.SigQCTestCaseID(prodname, testname, casename, True)
        
        # Read the data source name, function type and number of elements...
        self._datasource = ''.join(next(i_reader))
        self._functiontype = ''.join(next(i_reader))
        self._elements = int(''.join(next(i_reader)))
        
        # Skip the next line number because we're not sure what it is...
        row = next(i_reader)
        
        # Read the domain and range units...
        self._xunits = ''.join(next(i_reader))
        self._yunits = ''.join(next(i_reader))
        row = next(i_reader)
        self._line = i_reader.line_num    
    
    def getProdName(self):
        '''
        Returns the product name of the header object as a string.
        '''
        return self._testcase.GetProductName()
    
    def getTestName(self):
        '''
        Returns the test name of the header object as a string.
        '''
        return self._testcase.GetTestName()
    
    def getCaseName(self):
        '''
        Returns the case name of the header object as a string.
        '''
        return self._testcase.GetCaseName()
    
    def getDataSource(self):
        '''
        Returns the data source name of the header object as a string.
        '''
        return self._datasource
    
    def getFuntionType(self):
        '''
        Returns the function type of the header object as a string.
        '''
        return self._functiontype

    def getElements(self):
        '''
        Returns the number of elements of the header object as an integer.
        '''
        return self._elements
    
    def getXunits(self):
        '''
        Returns the x units of the header object as a string.
        '''
        return self._xunits

    def getYunits(self):
        '''
        Returns the y units of the header object as a string.
        '''
        return self._yunits    

###################################
# SigQCAsciiMatrix Class
###################################
class SigQCAsciiMatrix:
    '''
    The SigQCAsciiMatrix class is designed to encapsulate all of the information for a
    single SigQC test case data section enclosed within a BEGINDATA and ENDDATA
    section of the file.  The data information handles the test case domains, serial numbers,
    data from each serial numbered unit, as well as information from its associated header
    object.
    '''
    def __init__(self, i_reader=None):
        '''
        Constructor for an instance of the SigQCAsciiMatrix class used to optionally
        read the contents of the file at the first line after a BEGINDATA.
        
        Input:
            i_reader -  Optionally specify a CSV reader that will be used to fill
                        the contents of the header on construction.  The reader will
                        be used to read up to the point where the next ENDDATA is
                        encountered.
        '''
        self._line = -1
        self._serialnumbers = []
        self._timestamps = []
        self._xvalues = np.zeros(())
        self._yvalues = np.zeros(())
        self._header = None
        if (i_reader != None):
            self.read()
    
    def read(self, i_header, i_reader):
        '''
        Read the content of a data section within a SigQC ASCII text export file using the
        CSV file reader given.  This method assumes the reader has reached a BEGINDATA line
        and the next line to be read is the metadata (column names) for the data section.
        '''
        self._line = i_reader.line_num
        self._header = i_header
        
        # Read the domain values...
        xvals = []
        row = next(i_reader)
        for i in range(2,len(row)):
            xvals.append(row[i])
        
        # Read the serial numbers, timestamps, and test case data...
        yvals = []
        row = next(i_reader)
        while("ENDDATA" not in row):
            self._serialnumbers.append(row[0])
            self._timestamps.append(row[1])
            yvals.append(row[2::])
            row = next(i_reader)
        
        # Transpose the data such that columns (domains) become x axis values 
        #    and values from each unit become the y axis values
        self._xvalues = np.array(xvals)
        self._yvalues = np.array(yvals)

        row = next(i_reader)
        self._line = i_reader.line_num
        
    def getHeader(self):
        '''
        Get the data header that describes the data.  This includes the test case identifier,
        data source identifier, function type, number of elements plus the domain and range
        units of measure.  The returned object is an instance of the SigQCAsciiHeader class.
        '''
        return self._header
    
    def getXValues(self):
        '''
        Get the domain values of the data set.  The domain values (or x-values) apply to all
        measurements of the test case.
        
        Return:
           A one-dimensional numpy array of float values that represents the domain of the
           data.  There is one domain that applies to all data sets of the test case.
        '''
        return self._xvalues;
        
    def getYValues(self):
        '''
        Get the range values of the data for all production units represented in the test case.
        
        Return:
           A two-dimensional numpy array of float values that represents the data for multiple
           production unit measurements associated with the test case.  Each row of data represents
           a single production unit measurement.  Each column is associated with one domain value, and
           they are in the same order as the domain values.
        '''
        return self._yvalues
        
    def getYValuesAsSlices(self):
        '''
        Get the transpose of the range values of the data for all production units represented in
        the test case.  This can be considered as slices across all production units for each domain
        value.
        
        Return:
           A two-dimensional numpy array of float values that represents the data for multiple
           production unit measurements associated with the test case.  Each row of data represents
           the amplitude of production unit measurements at a specific domain value.  Each column
           is associated with one production unit.
        '''
        return self._yvalues.T
        
    def getSerialNumbers(self):
        '''
        Get the serial numbers of the test case.
        '''
        return self._serialnumbers
    
##################################
# SigQCAsciiLimits Class
##################################
class SigQCAsciiLimits:
    '''
    The SigQCAsciiLimits class is designed to encapsulate all of the information
    pertaining to the limits associated with each frequency domain value corresponding
    to a single SigQCDataMatrix.
    '''
    def __init__(self, i_reader=None):
        '''
        Constructor for an instance of the SigQCAsciiLimits used to optionally read
        the contents of the file at the first line after BEGINLIMITS until ENDLIMITS.
        '''
        self._lowerlimits = []
        self._upperlimits = []
        self._domains = []
        if (i_reader is not None):
            self.read(i_reader)

    def read(self, i_reader):
        '''
        Reads the limits of the dataset and sets the member variables.
        '''
        # Read the column names (metadata)...
        row = next(i_reader)
        self._domains.append(row)

        # Read the lower limits...
        row = next(i_reader)
        self._lowerlimits.append(row)

        # Read the upper limits...
        row = next(i_reader)
        self._upperlimits.append(row)

        # Skip the rest...
        while ("ENDLIMITS" not in row):
            row = next(i_reader)
            
    def getDomainValues(self):
        '''
        Get the domain values of the data set.  The domain values (or x-values) apply to all
        measurements of the test case.
        
        Return:
           A one-dimensional numpy array of float values that represents the domain of the
           data.  There is one domain that applies to all data sets of the test case.
        '''
        return self._domains
    
    def getLowerLimits(self):
        '''
        Get the lower limits of the data set. These limits apply to all measurements of the test
        case.
        
        Return:
            A one-dimensional numpy array of float values that represent the lower limits of the 
            data for each domain value (x-value). There is one set of lower limits that applies
            to all data sets of the test case.
        '''
        return self._lowerlimits
    
    def getUpperLimits(self):
        '''
        Get the upper limits of the data set. These limits apply to all measurements of the test 
        case.
        
        Return:
            A one-dimensional numpy array of float values that represent the upper limits of the 
            data for each domain value (x-value). There is one set of upper limits that applies to
            all data sets of the test case.
        '''
        return self._upperlimits

###################################
# SigQCAsciiTestCaseFile class
###################################
class SigQCAsciiTestCaseFile:
    '''
    The SigQCAsciiTestCaseFile class is designed to support reading of CSV files
    exported from SigQC using the "Test Case Data (ASCII) Export" feature.  Standard use
    of this class is as follows:
    
        x = SigQCAsciiTestCaseFile()       
        x.SetFilename("D:\MyData\MyAsciiTestCaseFile.csv" )
        x.Read()
    '''      
    def __init__(self,i_filename=None,i_delimiter=","):
        '''
        Constructor for a SigQCAsciiTestCaseFile class to open and read the content of
        a specified test case data file.  If a filename is specified, then the file is
        read within this constructor and ready for query after construction.
        
        Input:
            i_filename - Specify the fully-qualified path to the unit data file
                         to be read.  The default value is None.  In that case, the
                         file must be read after construction.
                         
            i_delimiter- String that contains the delimiter character.  By default,
                         the delimiter is a comma.
                         
        Example:
            x = SigQCAsciiTestCaseFile("D:\MyData\MyAsciiTestCaseFile.csv", "\t")
            
        OR
            x = SigQCAsciiTestCaseFile()       
            x.SetFilename("D:\MyData\MyAsciiTestCaseFile.csv" )
            x.SetDelimiter("\t")
            x.Read()
        '''
        self._filename = i_filename
        self._delimiter = i_delimiter
        self._casedata = []
        self._headerlist = []
        self._limits = []
        self._dataread = False
        if (self._filename is not None):
            self.read()
    def setFilename(self,i_filename):
        '''
        Set the fully qualified path to the unit data file to be read.
        
        '''
        self._filename = i_filename
        
    def setDelimiter(self,i_delimiter):
        '''
        Set the delimiter to be used when parsing the targeted unit
        data file.
        
        Input:
            i_delimiter-  String that contains the delimiter character.  By default
                          on construction, the delimiter is a comma.
                        
        Example:
            x = SigQCAsciiTestCaseFile()       
            x.SetFilename("D:\MyData\MyAsciiTestCaseFile.csv" )
            x.SetDelimiter("\t") # Use the tab delimiter instead
            x.Read()
                    
        '''
        self._delimiter = i_delimiter
        
    def read(self):
        '''
        Read the content of the targeted SigQC ASCII test case data file.
              
        Example:
            x = SigQCAsciiTestCaseFile()       
            x.SetFilename("D:\MyData\MyAsciiTestCaseFile.csv" )
            x.Read()
            
        '''
        # Indicate that an attempt has been made to read the test case data file...
        self._dataread = True
        line_num = 0
        
        # Initialize the member variables list...
        header = None
        data = None

        # Read the test case file...
        file = open(self._filename, 'r')
        reader = csv.reader(file)
        for row in reader:
            if ( "BEGINHEADER" in row):
                header = SigQCAsciiHeader()
                header.read(reader)
                self._headerlist.append(header)
            elif ("BEGINDATA" in row):
                data = SigQCAsciiMatrix()
                data.read(header, reader)
                self._casedata.append(data)
            elif ("BEGINLIMITS" in row):
                limits = SigQCAsciiLimits(reader)
                if (limits is not None):
                    self._limits.append(limits)
                    
        file.close()
        
    def getHeaders(self):
        '''
        Returns a list of SigQCAsciiHeader objects associated with the SigQCAsciiTestCaseFile.
        
        Example:
        x = SigQCAsciiTestCaseFile("D:\MyData\MyAsciiTestCaseFile.csv")
        headers = x.getHeaders()
        '''
        return self._headerlist
    
    def getIndexOfTestCase(self, i_testcaseid):
        '''
        Find the index of a specific test case within the file.
        '''
        index = -1
        for i in range(0,len(self._casedata)):
            if (self._casedata[i].getHeader()._testcase == i_testcaseid):
                index = i
                break
        return index;
       
    def getTestCaseCount(self):
        '''
        Returns the number of test cases that were contained in the file.
        '''
        return len(self._casedata);
        
    def getMatrixAt(self,i_index):
        '''
        Returns an instance of the SigQCAsciiMatrix class at the specified index. Each matrix represents
        data acquired on multiple production units associated with a single test case.
        
        Input:
           i_index - Specify the index of the test case whose data matrix is to be retrieved.
           
        Return:
           An instance of the SigQCAsciiMatrix class.
        '''
        return self._casedata[i_index]

    def getMatrixDataAt(self,i_index):
        '''
        Get the range values of the data for all production units represented in the test case.
        
        Input:
           i_index - Specify the index of the test case data matrix to be returned.
        
        Return:
           A two-dimensional numpy array of float values that represents the data for multiple
           production unit measurements associated with the test case.  Each row of data represents
           a single production unit measurement.  Each column is associated with one domain value, and
           they are in the same order as the domain values.
        '''
        return np.array(self._casedata[i_index].getYValues(), dtype=float)
    
    def getMatrixDataAsSlicesAt(self,i_index):
        '''
        Get the transpose of the range values of the data for all production units represented in
        the test case.  This can be considered as slices across all production units for each domain
        value.
        
        Input:
           i_index - Specify the index of the test case data matrix to be returned.
        
        Return:
           A two-dimensional numpy array of float values that represents the data for multiple
           production unit measurements associated with the test case.  Each row of data represents
           the amplitude of production unit measurements at a specific domain value.  Each column
           is associated with one production unit.
        '''
        return np.array(self._casedata[i_index].getYValuesAsSlices(), dtype=float)
    
    def getLimitsAt(self, i_index):
        '''
        Get the SigQCAsciiLimits object corresponding to the test case of the specified index.
        
        Input:
            i_index - Specify the index of the test case limits object to be returned.
        
        Return:
            A SigQCAsciiLimits object that encapsulates the set of lower and upper limits for each
            domain (x-value) corresponding to the test case specified by the index.
        '''
        return self._limits[i_index]

    def getMinMaxFeatures(self):
        '''
        Get the minimum and maximum number of features contained among all test case data sections.
        '''
        count = 0
        mincount = -1;
        maxcount = -1;
        for i in range(0,len(self._casedata)):
            count = len(self._casedata[i].getXValues())
            if (mincount == -1) or (count < mincount):
                mincount = count
            if (maxcount == -1) or (count > maxcount):
                maxcount = count
        return mincount, maxcount

    def getMinMaxUnits(self):
        '''
        Get the minimum and maximum number of units contained among all test case data sections.
        '''
        count = 0
        mincount = -1;
        maxcount = -1;
        for i in range(0,len(self._casedata)):
            count = len(self._casedata[i]._serialnumbers)
            if (mincount == -1) or (count < mincount):
                mincount = count
            if (maxcount == -1) or (count > maxcount):
                maxcount = count
        return mincount, maxcount

    
    def getAllTestCases(self):
        '''
        Get a tuple containing the matrix of all test case features of each unit and associated metadata.
        Units per test case must be consistent.
        
        Return:
            A tuple containing:
            1) A list of serial numbers of the units corresponding to axis 0 of the data matrix
            2) A list of domain names of the test case features
            3) A 2-D numpy array of results with serial numbers in rows and domain data in columns
            4) A 2-D numpy array containing lower and upper limits of test case features if present
        '''
        alldomainnames = []
        minunits, maxunits = self.getMinMaxUnits()
        minfeatures, maxfeatures = self.getMinMaxFeatures()
        allcasedata = np.empty((self.getTestCaseCount(), maxunits, maxfeatures))
        alllimits = np.empty((self.getTestCaseCount(), 2, maxfeatures))
        allserials = np.empty((self.getTestCaseCount(), maxunits), dtype=str)
        
        # Collect data
        for i in range(0,len(self._casedata)):
            matrix = self.getMatrixAt(i)
            data = self.getMatrixDataAt(i)
            limits = np.zeros((2,len(data[0,:])))
            allserials[i] = matrix.getSerialNumbers() 
            header = matrix.getHeader()
            if (len(data[0,:]) > 1):
                for j in range(0, len(data[0,:])):
                    alldomainnames.append(str(header.getCaseName())+" "+str(matrix.getXValues()[j]))
            else:
                alldomainnames.append(str(header.getCaseName())+" "+str(matrix.getXValues()[0]))
            allcasedata[i, 0:len(data), 0:len(data[0,:])] = data
            limits[0,:] = self.getLimitsAt(i).getLowerLimits()[0]
            limits[1,:] = self.getLimitsAt(i).getUpperLimits()[0]
            alllimits[i] = limits

        allcasedata = np.ma.masked_invalid(allcasedata)
        alllimits = np.ma.masked_invalid(alllimits)
        
        return (allserials, alldomainnames, allcasedata, alllimits)