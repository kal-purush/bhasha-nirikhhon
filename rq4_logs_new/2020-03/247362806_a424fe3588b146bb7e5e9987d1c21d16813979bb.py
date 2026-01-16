import argparse
import cv2
import numpy as np

from handle_models import handle_output, preprocessing
from inference import Network


CAR_COLORS = ["white", "gray", "yellow", "red", "green", "blue", "black"]
CAR_TYPES = ["car", "bus", "truck", "van"]

CPU_EXT = '/opt/intel/openvino/inference_engine/lib/intel64/libcpu_extension_sse4.so'


def get_args():
    parser = argparse.ArgumentParser("Basic Edge App with Inference Engine")

    c_desc = "CPU extension file location, if applicable"
    d_desc = "Device, if not CPU (GPU, FPGA, MYRIAD)"
    i_desc = "The location of the input image"
    m_desc = "The location of the model XML file"
    t_desc = "The type of model: POSE, TEXT or CAR_META"

    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    required.add_argument("-i", help=i_desc, required=True)
    required.add_argument("-m", help=m_desc, required=True)
    required.add_argument("-t", help=t_desc, required=True)
    optional.add_argument("-c", help=c_desc, default=None)
    optional.add_argument("-d", help=d_desc, default="CPU")
    args = parser.parse_args()

    return args


def get_mask(processed_output):
    empty = np.zeros(processed_output.shape)
    mask = np.dstack((empty, processed_output, empty))

    return mask


def create_output_image(model_type, image, output):
    if model_type == "POSE":
        output = output[:-1]
        for c in range(len(output)):    
            output[c] = np.where(output[c]>0.5, 255, 0)
        output = np.sum(output, axis=0)
        pose_mask = get_mask(output)        
        image = image + pose_mask
        return image
    elif model_type == "TEXT":
        output = np.where(output[0]>0.5, (0), 255)
        print(output[0][0])
        text_mask = get_mask(output)
        image = (image + text_mask)
        return image
    elif model_type == "CAR_META":
        color = CAR_COLORS[output[0]]
        car_type = CAR_TYPES[output[1]]
        print(color, car_type)
        scaler = max(int(image.shape[0] / 1000), 1)
        image = cv2.putText(image, 
            "Color: {}, Type: {}".format(color, car_type), 
            (50 * scaler, 100 * scaler), cv2.FONT_HERSHEY_SIMPLEX, 
            2 * scaler, (25, 0, 25), 3 * scaler)
        return image
    else:
        print("Unknown model type, unable to create output image.")
        return image


def perform_inference(args):
    inference_network = Network()
    n, c, h, w = inference_network.load_model(args.m, args.d, args.c)
    image = cv2.imread(args.i)
    preprocessed_image = preprocessing(args.t, image, h, w)
    inference_network.sync_inference(preprocessed_image)
    output = inference_network.extract_output()
    output_func = handle_output(args.t)
    processed_output = output_func(output, image.shape)
    output_image = create_output_image(args.t, image, processed_output)
    cv2.imwrite("{}-output.png".format(args.t), output_image)



def main():
    args = get_args()
    perform_inference(args)


if __name__ == "__main__":
    main()