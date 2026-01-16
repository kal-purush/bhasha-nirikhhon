from torchvision import transforms
import torchvision.transforms as T
from PIL import Image
import numpy as np
import cv2
import matplotlib.pyplot as plt
import torch.nn as nn
import torch.nn.functional as F
import torch
import sys
import argparse
import os
import facer

sys.path.append(os.path.join(sys.path[0], '..'))

# calc silh masks
from MODNet.src.models.modnet import MODNet
from tqdm import tqdm

# calc hair masks
from CDGNet.networks.CDGNet import Res_Deeplab

from copy import deepcopy

def dilate_mask(mask, kernel_size):
    # Create a kernel for dilation
    kernel = np.ones((kernel_size, kernel_size), np.uint8)

    dilated_mask = cv2.dilate(mask, kernel, iterations=1)

    return dilated_mask

def get_specific_mask(vis_seg_probs, type_class1, type_class2):
    mask = vis_seg_probs.detach().clone()
    # print('mask.size', mask.size)
    mask[mask == type_class1] = 255
    mask[mask == type_class2] = 255
    # mask[mask == type_class3] = 255
    mask[mask != 255] = 0
    mask = mask.cpu().numpy()
    mask = mask.astype(np.uint8)
    # mask = Image.fromarray(mask)
    return mask

def postprocess_mask(tensor):
    image = np.array(tensor) * 255.
    image = np.maximum(np.minimum(image, 255), 0)
    return image.astype(np.uint8)


def obtain_modnet_mask(im: torch.tensor, modnet: nn.Module,
                       ref_size=512, ):
    transes = [transforms.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))]
    im_transform = transforms.Compose(transes)
    im = im_transform(im)
    im = im[None, :, :, :]

    im_b, im_c, im_h, im_w = im.shape
    if max(im_h, im_w) < ref_size or min(im_h, im_w) > ref_size:
        if im_w >= im_h:
            im_rh = ref_size
            im_rw = int(im_w / im_h * ref_size)
        elif im_w < im_h:
            im_rw = ref_size
            im_rh = int(im_h / im_w * ref_size)
    else:
        im_rh = im_h
        im_rw = im_w
    im_rw = im_rw - im_rw % 32
    im_rh = im_rh - im_rh % 32
    im = F.interpolate(im, size=(im_rh, im_rw), mode='area')

    _, _, matte = modnet(im, True)
    # resize and save matte
    matte = F.interpolate(matte, size=(im_h, im_w), mode='area')
    matte = matte[0][0].data.cpu().numpy()
    return matte[None]


def valid(model, valloader, input_size, image_size, num_samples, gpus):
    model.eval()

    parsing_preds = np.zeros((num_samples, image_size[0], image_size[1]),
                             dtype=np.uint8)

    hpreds_lst = []
    wpreds_lst = []

    idx = 0
    interp = torch.nn.Upsample(size=(input_size[0], input_size[1]), mode='bilinear', align_corners=True)
    eval_scale = [0.66, 0.80, 1.0]
    # eval_scale=[1.0]
    flipped_idx = (15, 14, 17, 16, 19, 18)
    with torch.no_grad():
        for index, image in enumerate(valloader):
            # num_images = image.size(0)
            # print( image.size() )
            # image = image.squeeze()
            # if index % 10 == 0:
            #     print('%d  processd' % (index * 1))
            # ====================================================================================
            mul_outputs = []
            for scale in eval_scale:
                interp_img = torch.nn.Upsample(scale_factor=scale, mode='bilinear', align_corners=True)
                scaled_img = interp_img(image)
                # print( scaled_img.size() )
                outputs = model(scaled_img.cuda())
                prediction = outputs[0][-1]
                # ==========================================================
                hPreds = outputs[2][0]
                wPreds = outputs[2][1]
                hpreds_lst.append(hPreds[0].data.cpu().numpy())
                wpreds_lst.append(wPreds[0].data.cpu().numpy())
                # ==========================================================
                single_output = prediction[0]
                flipped_output = prediction[1]
                flipped_output[14:20, :, :] = flipped_output[flipped_idx, :, :]
                single_output += flipped_output.flip(dims=[-1])
                single_output *= 0.5
                # print( single_output.size() )
                single_output = interp(single_output.unsqueeze(0))
                mul_outputs.append(single_output[0])
            fused_prediction = torch.stack(mul_outputs)
            fused_prediction = fused_prediction.mean(0)
            fused_prediction = F.interpolate(fused_prediction[None], size=image_size, mode='bicubic')[0]
            fused_prediction = fused_prediction.permute(1, 2, 0)  # HWC
            fused_prediction = torch.argmax(fused_prediction, dim=2)
            fused_prediction = fused_prediction.data.cpu().numpy()
            parsing_preds[idx, :, :] = np.asarray(fused_prediction, dtype=np.uint8)
            # ====================================================================================
            idx += 1

    parsing_preds = parsing_preds[:num_samples, :, :]
    return parsing_preds, hpreds_lst, wpreds_lst


def mod_cdgnet(input_path):
    image_path = input_path
    target_path = '/home/neuralimage6/diffuser_x/temp_out_33/'
    os.makedirs(target_path, exist_ok=True)

    os.makedirs(os.path.join(target_path, 'mask'), exist_ok=True)
    os.makedirs(os.path.join(target_path, 'hair_mask'), exist_ok=True)
    os.makedirs(os.path.join(target_path, 'face_mask'), exist_ok=True)
    os.makedirs(os.path.join(target_path, 'ear_mask'), exist_ok=True)
    os.makedirs(os.path.join(target_path, 'nonear_face'), exist_ok=True)
    os.makedirs(os.path.join(target_path, 'mouth_mask'), exist_ok=True)

    #     load MODNET model for silhouette masks
    modnet = nn.DataParallel(MODNet(backbone_pretrained=False))
    MODNET_ckpt = '/home/neuralimage6/diffuser_x/MODNet/pretrained/modnet_photographic_portrait_matting.ckpt'
    modnet.load_state_dict(torch.load(MODNET_ckpt))
    device = torch.device('cuda')
    modnet.eval().to(device)


    basename1 = 'new1.jpg'
    basename2 = 'newface.jpg'
    print(basename1)
    # Create silh masks
    tens_list = []
    tens_list.append(T.ToTensor()(Image.open(image_path).convert("RGB")))
    silh_mask = obtain_modnet_mask(tens_list[0], modnet, 512)
    cv2.imwrite(os.path.join(target_path, 'mask', basename1), postprocess_mask(silh_mask)[0].astype(np.uint8))
    print("Start calculating hair masks!")


    # load CDGNet for hair masks

    model_cdg = Res_Deeplab(num_classes=20)
    CDGNET_ckpt = '/home/neuralimage6/diffuser_x/CDGNet/pretrained/LIP_epoch_149.pth'
    normalize = transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                     std=[0.229, 0.224, 0.225])

    transform = transforms.Compose([
        transforms.ToTensor(),
        normalize,
    ])

    state_dict = model_cdg.state_dict().copy()

    state_dict_old = torch.load(CDGNET_ckpt, map_location='cpu')

    for key, nkey in zip(state_dict_old.keys(), state_dict.keys()):
        if key != nkey:
            # remove the 'module.' in the 'key'
            state_dict[key[7:]] = deepcopy(state_dict_old[key])
        else:
            state_dict[key] = deepcopy(state_dict_old[key])

    model_cdg.load_state_dict(state_dict)
    model_cdg.eval()
    model_cdg.cuda()

    # basenames = sorted([s.split('.')[0] for s in os.listdir(os.path.join(args.scene_path, 'image'))])
    input_size = (1024, 1024)


    im = Image.open(image_path)
    img_size = im.size
    image_size = img_size

    images = []
    img = transform(im.resize(input_size))[None]
    img = torch.cat([img, torch.flip(img, dims=[-1])], dim=0)
    images.append(img)

    parsing_preds, hpredLst, wpredLst = valid(model_cdg, images, input_size, image_size, len(images), gpus=1)
    mask = np.asarray(Image.open(os.path.join(target_path, 'mask', basename1)))
    # print('img_size', img_size)
    print('parsing_preds', len(parsing_preds[0]))
    print(np.max(parsing_preds[0]))
    print(np.min(parsing_preds[0]))
    face_mask = np.asarray(Image.fromarray((parsing_preds[0] == 13)).resize(img_size, Image.BICUBIC))
    glass_mask = np.asarray(Image.fromarray((parsing_preds[0] == 0)).resize(img_size, Image.BICUBIC))
    glass_mask1 = np.asarray(Image.fromarray((parsing_preds[0] == 4)).resize(img_size, Image.BICUBIC))
    dress_mask = np.asarray(Image.fromarray((parsing_preds[0] == 5)).resize(img_size, Image.BICUBIC))   # dress mask
    upper_cloth = np.asarray(Image.fromarray((parsing_preds[0] == 6)).resize(img_size, Image.BICUBIC))  # upper-clothes
    jumpsuit = np.asarray(Image.fromarray((parsing_preds[0] == 10)).resize(img_size, Image.BICUBIC))  # jumpsuit
    scarf = np.asarray(Image.fromarray((parsing_preds[0] == 11)).resize(img_size, Image.BICUBIC))  # scarf
    skirt = np.asarray(Image.fromarray((parsing_preds[0] == 12)).resize(img_size, Image.BICUBIC))  # skirt
    left_arm = np.asarray(Image.fromarray((parsing_preds[0] == 14)).resize(img_size, Image.BICUBIC))  # left arm
    right_arm = np.asarray(Image.fromarray((parsing_preds[0] == 15)).resize(img_size, Image.BICUBIC)) # right arm
    glove_mask = np.asarray(Image.fromarray((parsing_preds[0] == 3)).resize(img_size, Image.BICUBIC)) # glove
    coat_mask = np.asarray(Image.fromarray((parsing_preds[0] == 7)).resize(img_size, Image.BICUBIC))   # coat
    hat_mask = np.asarray(Image.fromarray((parsing_preds[0] == 1)).resize(img_size, Image.BICUBIC))  #  hat

    hair_mask = np.asarray(Image.fromarray((parsing_preds[0] == 2)).resize(img_size, Image.BICUBIC))
    other_mask = np.asarray(Image.fromarray((parsing_preds[0] != 2)).resize(img_size, Image.BICUBIC))
    hair_mask = hair_mask * mask
    # glass_mask = glass_mask*mask
    mask_path = os.path.join(target_path, 'hair_mask', basename1)
    print(hair_mask.size)
    Image.fromarray(hair_mask).save(mask_path)

    face_mask = face_mask * mask + glass_mask1*mask
    mask_path1 = os.path.join(target_path, 'face_mask', basename1)
    print(face_mask.size)
    Image.fromarray(face_mask).save(mask_path1)


    mask1 = dress_mask*mask + jumpsuit*mask + scarf*mask + skirt*mask + left_arm*mask + right_arm*mask + glove_mask*mask + coat_mask*mask + hat_mask*mask + upper_cloth*mask
    other_mask = other_mask * mask - mask1
    other_mask[other_mask <= 200] = 0
    os.makedirs(os.path.join(target_path, 'other_mask'), exist_ok=True)
    mask_path2 = os.path.join(target_path, 'other_mask', basename1)
    Image.fromarray(other_mask).save(mask_path2)
    print('Image.fromarray(other_mask).size', Image.fromarray(other_mask).size)

    img1 = cv2.imread(mask_path2)  ### other mask (beside hair)
    # print('img1', img1.shape)  #### still working on mask operation ####
    # print('img1', img1)
    # print(max(np.where(img1.all() >= 200))[1])

    # other = Image.fromarray(other_mask)
    img2 = cv2.imread(mask_path1)  ### face mask
    facemsk = img2.astype(np.uint8)
    new_img = cv2.bitwise_xor(img1, img2)
    basename3 = 'xor-mask.jpg'
    mask_path11 = os.path.join(target_path, 'face_mask', basename3)
    cv2.imwrite(mask_path11, new_img)
    # new_img.cpu().numpy()
    new_img = new_img.astype(np.uint8)

    # new_img= dilate_mask(new_img, 5)
    # print('new_img', new_img)

    # print('new_img[new_img==255]', max(np.where(new_img > 200)[1]))
    # print('facemsk[new_img==255]x_max', max(np.where(facemsk > 200)[1]))
    # print('facemsk[new_img==255]x_min', min(np.where(facemsk > 200)[1]))
    #
    # print('facemsk[new_img==255]y_max', max(np.where(facemsk > 200)[0]))
    # print('facemsk[new_img==255]y_min', min(np.where(facemsk > 200)[0]))


    # print('facemsk[new_img==255]x', max(np.where(facemsk >= 200)[0]))
    lowest_point_of_face = int(min((np.where(facemsk > 200)[0]) + max(np.where(facemsk > 200)[0])) / 2) + int(
        .2 * (max((np.where(facemsk > 200)[0]) - min(np.where(facemsk > 200)[0]))))
    high_point_of_face = min((np.where(facemsk > 200)[0]))+ int(
        .1 * (max((np.where(facemsk > 200)[0]) - min(np.where(facemsk > 200)[0]))))
    # print('lowest_point_of_face', lowest_point_of_face)
    # lowest_point_of_face_x = int(min((np.where(facemsk >= 200)[0]) + max(np.where(facemsk >= 200)[1])) / 2) + int(
    #     .4 * (max((np.where(facemsk >= 200)[1]) - min(np.where(facemsk >= 200)[1]))))
    # position = min(min(np.where(facemsk >= 200)[1]), lowest_point_of_face)
    position = lowest_point_of_face
    # new_img[position:, :] = 0
    img1[position:, :] = 0
    img1[:high_point_of_face, :] = 0
    # img1[position:, :min(np.where(facemsk > 200)[1])] = 0
    # img1[position:, max(np.where(facemsk > 200)[1]):] = 0
    img1[:, :min(np.where(facemsk > 200)[1])] = 0
    img1[:, max(np.where(facemsk > 200)[1]):] = 0
    face = cv2.bitwise_or(img1, img2)
    # face[position:, :] = 0
    face[face > 200] = 255
    mask_path_3 = os.path.join(target_path, 'face_mask', basename2)
    face = Image.fromarray(face)
    # print(face.size)
    face.save(mask_path_3)

    h_mask = Image.open(mask_path)
    print('h_mask.size', h_mask.size)
    f_mask = Image.open(mask_path_3)
    print('f_mask.size', f_mask.size)
    b_mask = Image.open(os.path.join(target_path, 'other_mask', basename1))

    # get rid off ear mask
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    image = facer.hwc2bchw(facer.read_hwc(image_path)).to(device=device)  # image: 1 x 3 x h x w
    face_detector = facer.face_detector('retinaface/mobilenet', device=device)
    with torch.inference_mode():
        faces = face_detector(image)

    face_parser = facer.face_parser('farl/celebm/448', device=device)  # optional "farl/lapa/448"
    with torch.inference_mode():
        faces = face_parser(image, faces)
    seg_logits = faces['seg']['logits']
    seg_probs = seg_logits.softmax(dim=1)  # nfaces x nclasses x h x w
    # n_classes = seg_probs.size(1)
    vis_seg_probs = seg_probs.argmax(dim=1)  # /n_classes*255
    # Include face, (hair) and face parts in the mask
    # 1: Face, 2: Left Eyebrow, 3: Right Eyebrow, 4: Left Eye, 5: Right Eye, 6: Nose, 7: Upper Lip, 8: Inner Mouth, 9: Lower Lip, 10: Hair
    # celeba
    # '1: neck, 2: face, 3: cloth, 4: right rainbow, 5: left ear, 6: right ear, 7: left brow, 8: right eys, 9: left eye, 10:nose, 11: mouth, 12: low lip, 13:upper lip, 14:hair, 15: eye galsses, 16: hat, 17: ear ring, 18: neck_low
    ## mouth mask
    mouth_mask = get_specific_mask(vis_seg_probs, 11, 11)
    mouth_mask = np.expand_dims(dilate_mask(np.squeeze(mouth_mask[0]), 1), 0)
    # ear_mask = get_specific_mask(vis_seg_probs, 4, 5)
    # ear_mask = np.expand_dims(dilate_mask(np.squeeze(ear_mask[0]), 1), 0)


    # mask_path4 = os.path.join(target_path, 'ear_mask', basename1)
    mask_path5 = os.path.join(target_path, 'mouth_mask', basename1)

    # print(ear_mask.size)
    # Image.fromarray(ear_mask.reshape(img_size)).save(mask_path4)
    Image.fromarray(mouth_mask.reshape(img_size)).save(mask_path5)
    # e_mask = Image.open(mask_path4)
    m_mask = Image.open(mask_path5)
    # print('e_mask.size', e_mask.size)
    # earmask = np.array(earmask)
    # print('earmask.size', e_mask.size)
    print('np.array(face_mask).size', np.array(face_mask).size)
    image2 = cv2.imread(mask_path_3)  ### face mask
    # facemsk = img2.astype(np.uint8)
    # image1 = cv2.imread(mask_path4)  ### ear mask
    image3 = cv2.imread(mask_path5)  ### ear mask
    # image1 = dilate_mask(image1, kernel_size=15)
    # earmsk = image1.astype(np.uint8)

    # nonearface = cv2.bitwise_xor(image2, image1)
    # mask_path6 = os.path.join(target_path, 'nonear_face', basename1)
    # nonear_face = Image.fromarray(nonearface)
    # nonear_face.save(mask_path6)
    # image4 = cv2.imread(mask_path6)
    nonearfacemouth = cv2.bitwise_or(image2, image3)
    mask_path7 = os.path.join(target_path, 'nonear_face', basename2)
    nonearface_mouth = Image.fromarray(nonearfacemouth)
    nonearface_mouth.save(mask_path7)

    f_mask = Image.open(mask_path7)



    return h_mask, f_mask, b_mask


#
# a = '/home/neuralimage6/diffuser_x/temp1.png'  #male_h_36.jpg
# mod_cdgnet(a)
# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(conflict_handler='resolve')
#
#     parser.add_argument('--scene_path', default='/home/neuralimage6/diffuser_x/11/1_F.jpg', type=str)
#     parser.add_argument('--MODNET_ckpt', default='/home/neuralimage6/diffuser_x/MODNet/pretrained/modnet_photographic_portrait_matting.ckpt',
#                         type=str)
#     parser.add_argument('--CDGNET_ckpt', default='/home/neuralimage6/diffuser_x/CDGNet/pretrained/LIP_epoch_149.pth', type=str)
#
#     args, _ = parser.parse_known_args()
#     args = parser.parse_args()
#
#     main(args)