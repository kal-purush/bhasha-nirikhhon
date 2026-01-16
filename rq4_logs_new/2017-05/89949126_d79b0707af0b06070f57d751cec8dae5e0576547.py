import sys


colornames ={
"Black": "ColorTable08",
"BoldBlack": "ColorTable00",
"Red": "ColorTable12",
"BoldRed": "ColorTable04",
"Green": "ColorTable10",
"BoldGreen": "ColorTable02",
"Yellow": "ColorTable15",
"BoldYellow": "ColorTable06",
"Blue": "ColorTable09",
"BoldBlue": "ColorTable01",
"Magenta": "ColorTable13",
"BoldMagenta": "ColorTable05",
"Cyan": "ColorTable11",
"BoldCyan": "ColorTable03",
"White": "ColorTable07",
"BoldWhite": "ColorTable15" 
} 


arg1 = 'mintty_color.txt'
arg2 = 'test3.txt'


try:
    __IPYTHON__
    arg1 = 'mintty_color.txt'
    arg2 = 'test3.txt'
except:
    if len(sys.argv)==3:
        arg1 = sys.argv[1]
        arg2 = sys.argv[2]
    else:
        print('usage:',sys.argv[0],'<infile> <outfile>')


def convert_line(line):
    l = line.strip()
    if (l.count('=')==1 and l.count(',')==2 
        and not l[0] in '#;'):
        z = l.find('=')
        name = l[:z].strip()
        rgb = [int(x.strip()) for x in l[z+1:].split(',')]
        d = colornames.get(name,None)
        if d:
            return ('"{}"=dword:00{:02X}{:02X}{:02X}'.format(d,*reversed(rgb)))
    return None
 
    
def convert_file(fromfile, tofile):
    with  open(tofile,'w') as outfile:
        print('Windows Registry Editor Version 5.00',file =outfile);
        print('[HKEY_CURRENT_USER\Console]',file = outfile)
        with open(fromfile) as f:
            for line in f:
                regline = convert_line(line)
                if regline:
                    print(regline,file = outfile)
                    
    
if __name__ == '__main__':
    convert_file(arg1,arg2)