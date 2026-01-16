from src.antlr.antlr_helper import TreeHelper
import argparse
from antlr4 import FileStream

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', help='Path to query script file')
    parser.add_argument('-o', '--output', help='Path to output visualization (.dot file)')
    parser.add_argument('-v', '--view', action='store_true', help='Open visualization')
    args = parser.parse_args()
    input_stream = FileStream(args.input)
    tree_helper = TreeHelper(input_stream)
    tree_helper.get_visualization(args.output, show_view=args.view)