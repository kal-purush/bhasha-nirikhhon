

def Request_File_Name(suffix = '.txt', defaultName = 'log.txt'):
    '''Ask for a file name input by console'''
    val = raw_input("Input your file name:(default is '"+defaultName+"')")
    if not val:
        val = defaultName
    else:
        if suffix != val[-len(suffix):]:
            val = val + suffix
    return val





def Pick_Your_Log(LogFile, TargetLog):
    '''Choose specific logs from log file
    
    Arguments:
    LogFile -- a string containing file name , or file path
    TargetLog -- a list containing target logs. each item present a string target
    '''
    # Transform targets to known rules
    Target_List = []
    for i in TargetLog:
        Target_List.append(Target_Rule(i))

    # Read raw file
    f = open(LogFile,'r')
    output = open(LogFile[:-4]+'_new.txt','w')

    for line in f:
        for target in Target_List:
            if (Check_Match(line, target)):
                # if match any target, write to file
                output.write(line)
                break
    # close file 
    f.close()
    output.close()

def Target_Rule(log):
    '''Transform target rule and return a tuple'''
    must   = []
    ignore = []
    value  = []
    for i in range(len(log)):
        if '*' == log[i]:
            value.append(i)
        elif '?' == log[i]:
            ignore.append(i)
        else:
            must.append(i)
    return (must, value, ignore, log)

def Check_Match(raw, target):
    '''If all MUST bit fits the rule, return 1(match)'''
    must, _, _, log = target
    if len(raw) < len(log):
        return 0
    status = 1
    for i in must:
        if log[i] != raw[i]:
            status = 0
            break
    return status

if __name__ == "__main__":
    print "Test"
    TargetLog = ['[R] PAA[CH*|CE*|LN*|PN*|BK0x***|PG0x***] BusyTime[****]']
    Pick_Your_Log("test.log", TargetLog)