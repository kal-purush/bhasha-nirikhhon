import sys

def to_upper(input_str):
    return input_str[0:5]

if __name__ == '__main__':
    input_str = sys.argv[1]
    output_str = to_upper(input_str)
    print(output_str)