import netbyte
import sys

nbe = netbyte.Netbyte()

open(sys.argv[2], "wb").write(nbe.compile(*nbe.parse_file(sys.argv[1])))