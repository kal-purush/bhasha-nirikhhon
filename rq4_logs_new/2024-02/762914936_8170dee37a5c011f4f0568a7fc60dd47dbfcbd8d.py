__import__('sys').path.append(__import__('os').path.dirname(__file__))

import sys
import shell

def main():
    if len(sys.argv) == 2:
        shell.run(sys.argv[1])
    elif len(sys.argv) == 1:
        shell.main()
    else:
        print("Blixyr > Invalid Arguments")

if __name__ == '__main__':
    main()