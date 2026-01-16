import pandas as pd
import sys, getopt

def main(argv):
    inputfile = ''
    try:
        opts, args = getopt.getopt(argv,"hi:o:",["ifile="])
    except  getopt.GetoptError:
        print('generate-tables-script.py -i <inputfile> ')

    for opt, arg in opts:
        if opt == '-h':
             print('test.py -i <inputfile> ')
             sys.exit()
        elif opt in ("-i", "--ifile"):
            inputfile = arg
    print('Input file is ', inputfile)


    df = pd.read_csv(inputfile)

    #renaming and cleaning up some columns, converting to datetime
    df['PostSubTime'] = pd.to_datetime(df['Post Submission Time'] )
    df['PostStartTime'] = pd.to_datetime(df['Post Start Time'] )
    df['PostCompTime'] = pd.to_datetime(df['Post Complete Time'] )
    df = df[['Bin','PostSubTime','PostStartTime','PostCompTime','Operation']]

    #defining new columns for analysis
    df['Waittime'] = df['PostStartTime'].shift(periods=-1) - df['PostCompTime'] # need to shift to subtract last complete time
    df['PostTime'] = df['PostCompTime'] - df['PostStartTime']
    df['PostStartHour'] = df.PostStartTime.dt.hour


    #First Table:
    first = df.groupby(df.PostStartHour).agg({'Bin':['count'],
                                  'Waittime':lambda x: x.astype('timedelta64[s]').mean(),
                                 'PostTime':lambda x: x.astype('timedelta64[s]').mean()})
    first.columns = ['Bin','AvgWaitTime','AvgPostTime']
    first = first.reset_index()
    first.to_csv('Hour_summary.csv')

    #Second Table:
    df2 = df.groupby('Operation').agg({'PostTime':lambda x: x.astype('timedelta64[s]').mean()})
    df2.to_csv('create_edit.csv')


    #Third Table:

    def funct(row):
        if row['Operation']=='CREATE':
            return row['Bin']/createtotal
        else:
            return row['Bin']/edittotal
            
    lst = df.groupby(['Operation','Bin'])['PostSubTime'].count()
    lst.name = 'Count'
    lst = lst.reset_index()
    lst.to_csv('bin_operation.csv')
    createtotal = df[df.Operation=='CREATE']['Bin'].count()
    edittotal = df[df.Operation=='EDIT']['Bin'].count()
    lst['Percent'] = lst.apply(lambda row: funct(row), axis=1)
    lst.to_csv('percent.csv')



    #Last Table:

    newdf = df.groupby(['PostStartHour','Operation','Bin'])['PostSubTime'].count()
    newdf.name = 'Count'
    newdf = newdf.reset_index()
    newdf.to_csv('hour_operation_bin.csv')
    print('created last table')


if __name__ == "__main__":
    main(sys.argv[1:])