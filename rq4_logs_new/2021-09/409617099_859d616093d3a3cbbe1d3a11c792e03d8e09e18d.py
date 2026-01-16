import argparse

def repeat_char(char, num_of_times):
    return (char * (num_of_times//len(char) + 1))[:num_of_times]

def main():
    parser = argparse.ArgumentParser(description='Repeat a character number of times.')
    required = parser.add_argument_group('Required arguments')
    optional = parser.add_argument_group('Optional arguments')
    required.add_argument("-c", "--c", metavar='C', type=str, required=True,
                        default=None, help='character to repeat')
    optional.add_argument("-n", "--n", metavar='N', type=int, default=6,
                        help='repeat number of times')
    optional.add_argument("-o", "--o", metavar='out', type=str, default=False,
                        help='PATH to output file')
    args = parser.parse_args()

    if args.o:
        with open(args.o, "w") as f:
            f.write(repeat_char(args.c, args.n))
    else:
        print(repeat_char(args.c, args.n))

if __name__ == '__main__':
    main()