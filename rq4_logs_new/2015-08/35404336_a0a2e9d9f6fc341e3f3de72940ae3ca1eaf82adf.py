def main():
    import os
    import sys

    os.setuid(0)
    os.execv("/usr/bin/gdb", sys.argv)

if __name__ == '__main__':
    main()