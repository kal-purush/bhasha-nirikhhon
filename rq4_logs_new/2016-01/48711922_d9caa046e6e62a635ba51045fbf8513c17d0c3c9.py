import argparse

if __name__ == '__main__' :

    parser = argparse.ArgumentParser(description='Program description')
    parser.add_argument('input', help='Input file')
    parser.add_argument('-o', '--option', type=int, help='Integer option')
    args = parser.parse_args()
  