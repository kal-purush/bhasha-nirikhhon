import sys
from io import IOBase, StringIO, BytesIO
__all__ = """
fputs
puts
fprintf
printf
"""[1:-1].splitlines(False)
def fputs(string, stream):
    finalstr = str(string)
    print(finalstr, end="", file=stream)
    return len(finalstr)
def puts(string):
    return fputs(string, sys.stdout)
def fprintf(stream, fmt, *a):
    finalstr = fmt % a
    print(finalstr, end="", file=stream)
    return len(finalstr)
def printf(fmt, *a):
    return fprintf(sys.stdout, fmt, *a)

if __name__ == "__main__":
    puts("Hello world!\n")
    printf("%s\n", "Hello world!")
    x, y, z = 1, 2, 3
    printf("x = %s\ny = %s\nz = %s\n", x, y, z)
    with open("pystdio_test_output.txt", "w") as f:
        fputs("Hello world!\n", f)
        fprintf("%s %s\n", "Formatted string", 123)