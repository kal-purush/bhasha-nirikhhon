from arguments import TransformerCRFArguments as arg
from test_module import read_labels
from module import build_model
import torch
import os
import pickle
import argparse
import time
from ner.test_module import read_labels, run_real

first_time = time.time()
if __name__ == '__main__':
    def arg_manager():
        parser = argparse.ArgumentParser()
        parser.add_argument("--choosen_dataset", type=str,
                            default="processed_8")  # processed_8 refers to the largest model.
        parser.add_argument("--text", type=str, default="")
        return parser.parse_args()
    args = arg_manager()

    dataset = args.choosen_dataset
    # read file order
    labels = read_labels()
    print('Look up', arg.lookup_path)
    lookup = None
    with open(arg.lookup_path, 'rb') as fin:
        lookup = pickle.load(fin)
    word2idx = lookup['word2idx']
    arg.num_vocabs = len(word2idx)
    model = build_model(arg.model_name, arg).to(arg.device)
    # Load existed weights
    if os.path.exists(arg.test_ckpt):
        model.load_state_dict(torch.load(arg.test_ckpt))
    else:
        exit()

    text_input = args.text
    labels = read_labels()
    run_real(text_input, labels, arg, model, lookup)
    out = ' Running time : ' + str(time.time() - first_time) + ' seconds'
    print(out)