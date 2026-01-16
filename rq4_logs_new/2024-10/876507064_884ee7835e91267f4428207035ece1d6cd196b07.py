import splitfolders
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--input_dir', type=str, default='./train/train')
parser.add_argument('--output_dir', type=str, default='./data')
parser.add_argument('--split_ratio', type=float, default=0.9)
args = parser.parse_args()


print(f'input_dir: {args.input_dir}')
print(f'output_dir: {args.output_dir}')
print(f'split_ratio: {args.split_ratio}')

print('Splitting data...')
splitfolders.ratio(args.input_dir, output=args.output_dir,seed=1337, ratio=(args.split_ratio, 1-args.split_ratio), group_prefix=None) # default values
print('Done!')