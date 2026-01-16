import argparse


def print_mad_libs(color, plural_noun, celebrity):
    print(f'Roses are {color}')
    print(f'{plural_noun} are blue')
    print(f'I love {celebrity}')

def get_mad_libs_from_user():
    color = input("Enter a color:")
    plural_noun = input("Enter a plural noun:")
    celebrity = input("Enter a celebrity:")
    return color, plural_noun, celebrity

def parse_args():
    parser = argparse.ArgumentParser(description='color, noun, celebrity')
    # parser.add_argument('passed_info', type=str, nargs='+')
    parser.add_argument('--color', type=str, required=False)
    parser.add_argument('--plural_noun', type=str, required=False)
    parser.add_argument('--celebrity', type=str, required=False)

    args = parser.parse_args()
    return args
def get_mad_libs_from_args(args):
    return  args.color, args.plural_noun, args.celebrity


if __name__ == '__main__':
    # color, plural_noun, celebrity = get_mad_libs_from_user()
    args = parse_args()
    color, plural_noun, celebrity = get_mad_libs_from_args(args)
    if color is None:
        color, plural_noun, celebrity = get_mad_libs_from_user()



    print_mad_libs(color, plural_noun, celebrity)