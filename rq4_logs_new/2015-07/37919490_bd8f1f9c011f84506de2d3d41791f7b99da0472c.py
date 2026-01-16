import string
import urllib
import gzip  
import os 
import gzip
from ftplib import FTP

# plotfile = open('plot.list.gz','w')

ftp = FTP('ftp.fu-berlin.de')
ftp.login()
ftp.cwd('/pub/misc/movies/database/')
filename1 = 'plot.list.gz'
filename2 = 'genres.list.gz'
localfile1 = open(filename1,'wb')
localfile2 = open(filename2,'wb')
ftp.retrbinary('RETR ' + filename1, localfile1.write)
ftp.retrbinary('RETR ' + filename2, localfile2.write)
ftp.quit()
localfile1.close()
localfile2.close()

gzPlot = gzip.GzipFile(fileobj = open(r'plot.list.gz','rb'))
un_gzPlot = open('originalPlot.list','w')
un_gzPlot.write(gzPlot.read())
un_gzPlot.close()

gzGenres = gzip.GzipFile(fileobj = open(r'genres.list.gz','rb'))
un_gzGenres = open('genres.list','w')
un_gzGenres.write(gzGenres.read())
un_gzGenres.close()

outputfile = open('plot.list','w')

plot = ''
with open('originalPlot.list') as infile:
    for line in infile:
        if (line[0] != '\n'):
            if(line[0] != 'B'):
                if (line[0] == '-' and len(plot) != 0):
                    plot += '\n'
                    outputfile.write(plot)
                    plot = ''
                    continue              
                else:
                    line = line.rstrip()
                    if(line[0:3]=="MV:"):
                        plot += line[4:]
                        plot += ":::"
                    else:
                        if(line[0:3]=="PL:"):
                            plot += line[4:]
                            plot += ' '
outputfile.close()