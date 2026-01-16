from read_xray_once import read_image
from build_xray_dataset import Xray_dataset
from build_xray_models import Ensemble
from torch.utils.data import DataLoader
import torch
import numpy as np

device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")


class Test_xray(object):
    def __init__(self, paras, dnn_para, device=None):
        """
        test and classify a X-Ray using trained Ensemble.
        :param paras: [parameter of ResNet, of vgg]
        :param dnn_para: parameter of DNN of Ensemble
        :param device:
        """
        super(Test_xray, self).__init__()
        self.en = Ensemble(paras, device, dnn_para).eval()  # due to BatchNorm

    def test_one_xray(self, image, ):
        img = np.array(read_image(image))
        # due to BatchNorm1d, #data should be more than 1.
        # img = np.concatenate((img, img), 0)
        placeholder = np.ones(shape=(img.shape[0], 1))
        testset = Xray_dataset([img, placeholder], is_train=False)
        testloader = DataLoader(testset, batch_size=img.shape[0],  # batch_size=1
                            shuffle=True, pin_memory=True, drop_last=True)
        with torch.no_grad():
            for i, (data, _) in enumerate(testloader):
                data = data.to(device)
                # label = label.type(torch.LongTensor).to(device)

                pred = self.en(data)
                # pred = self.en(data)[0]  # pred[0] and pred[1] are same.
                prediction = torch.argmax(pred, dim=1)
        label = prediction.numpy()[0]
        if label == 0:
            return 'Normal'
        elif label == 1:
            return 'COVID-19'
        elif label == 2:
            return 'Pneumonia'


if __name__ == '__main__':
    # xray_filename = './original_data/X-Ray/Coronahack-Chest-XRay-Dataset/'\
    #                  'Coronahack-Chest-XRay-Dataset/test/IM-0001-0001.jpeg'
    xray_filename = './original_data/Data/test/COVID19/COVID19(460).jpg'

    test_xray = Test_xray(paras=['./dataset/xray_res_e50_b64_lr0001_para.pt',
                          './dataset/xray_vgg_e50_b64_lr0001_para.pt'],
                          dnn_para='./dataset/xray_dnn_e50_b64_batchn_l2_para.pt',
                          device=device)
    pred = test_xray.test_one_xray(xray_filename, )
    