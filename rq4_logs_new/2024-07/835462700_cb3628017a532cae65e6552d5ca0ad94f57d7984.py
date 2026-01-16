import os
import sys
from pathlib import Path
import logging
import torch
import argparse
from datetime import datetime
from tqdm import tqdm
from PIL import Image

from utils.datasets_2 import DisasterDataset

from models.end_to_end_Siam_UNet import SiamUnet

logging.basicConfig(stream=sys.stdout,
                    level=logging.INFO,
                    format='{asctime} {levelname} {message}',
                    style='{',
                    datefmt='%Y-%m-%d %H:%M:%S')

def load_dataset(input_dir):
    img_suffix_list = list(input_dir.glob('*_post_disaster.tif'))
    img_suffix_list = [img_suffix.name.split('_post_disaster.tif')[0]  for img_suffix in img_suffix_list]

    dataset = DisasterDataset(data_dir=input_dir, img_suffix_list=img_suffix_list, transform=False, normalize=True)
    logging.info('number of images to run inference'.format(len(dataset)))

    return dataset

def run_inference(dataset, model, device, output_dir):

    softmax = torch.nn.Softmax(dim=1)
    model.eval()
    with torch.no_grad():
        for i in tqdm(range(len(dataset))):
            data = dataset[i]
            pre_img = data['pre_image'].unsqueeze(0).to(device)
            post_img = data['post_image'].unsqueeze(0).to(device)

            output = model(pre_img, post_img)
            output = softmax(output)
            output = output.cpu().numpy()

            output = output.squeeze(0)
            output = output.transpose(1, 2, 0)
            output = output.argmax(axis=-1)

            output = Image.fromarray(output.astype('uint8'))
            output.save(output_dir.joinpath(f'{dataset.img_suffix_list[i]}_prediction.tif'))
    pass

def main(args):
    
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger_dir = output_dir.joinpath('logs')
    logger_dir.mkdir(parents=True, exist_ok=True)


    logging.info(f'Using PyTorch version {torch.__version__}.')
    device = torch.device(args.gpu if torch.cuda.is_available() else "cpu")
    logging.info(f'Using device: {device}.')

    #load model and its state from the given checkpoint
    model = SiamUnet()
    checkpoint_path = args.model
    if checkpoint_path and os.path.isfile(checkpoint_path):
        logging.info('Loading checkpoint from {}'.format(checkpoint_path))
        checkpoint = torch.load(checkpoint_path, map_location=device)
        model.load_state_dict(checkpoint['state_dict'])
        model = model.to(device=device)
        logging.info(f"Using checkpoint at epoch {checkpoint['epoch']}, val f1 is {checkpoint.get('val_f1_avg', 'Not Available')}")
    else:
        logging.info('No valid checkpoint is provided.')
        return

    logging.info(f'Starting model inference ...')
    inference_start_time = datetime.now()

    logging.info(f'Loading dataset from {args.input_dir} ...')
    dataset = load_dataset(args.input_dir)

    logging.info(f'Running inference on the dataset ...')
    run_inference(dataset, model, device, output_dir)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Building Damage Assessment Inference')
    parser.add_argument('--output_dir', type=str, required=True, help='Path to an empty directory where outputs will be saved. This directory will be created if it does not exist.')
    parser.add_argument('--input_dir', type=str, required=True, help='Path to a directory that contains input images.')
    parser.add_argument('--model', type=str, required=True, help='Path to a trained model to be used for inference.')
    parser.add_argument('--gpu', type=str, default="cuda:0", help='GPU to run on.')
    args = parser.parse_args()
    
    main(args)