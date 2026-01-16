"""
This modlue is for base merge and return the mergebase with Upper Case
"""
import sys
baseDict={frozenset("AG"):'R' , frozenset('CT'):'Y' , frozenset('AC'):'M' ,
          frozenset('GT'):'K' , frozenset('GC'):'S' , frozenset('AT'):'W' ,
          frozenset('ATC'):'H', frozenset('GTC'):'B', frozenset('GAC'):'V',
          frozenset('GAT'):'D', frozenset('ATCG'):'N'}

def merge(baseset):
    """

    :rtype: str
    """
    return baseDict[baseset]

def main(baseset):
    print merge(baseset)

if __name__ == '__main__':
    sys.exit(main())