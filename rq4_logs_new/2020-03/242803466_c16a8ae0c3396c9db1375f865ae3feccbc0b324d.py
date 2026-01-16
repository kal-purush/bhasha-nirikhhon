import torch 
import torch.nn as nn
import torch.nn.functional as F
import numpy as np
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt
from sklearn.preprocessing import label_binarize
from torchvision import datasets, transforms
import torchvision
import pdb
from sklearn.metrics import roc_curve, auc


test_loader = torch.utils.data.DataLoader(
datasets.ImageFolder("test",
transform = transforms.Compose([transforms.Resize((256,256)), transforms.ToTensor(), transforms.Normalize(mean=[0.485,0.456,0.406],std=[0.229,0.224,0.225])]) #reshape to 512x512 b/c they were unequal and they're all around 512
),
batch_size = 10, shuffle = False
)

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

model = torch.load('BestModel.pth')

def eval(): #evaluates test set 
	with torch.no_grad():
		model.eval() #turns off batch norm and dropout and any other training only regulizers etc
		acc_sum = 0
		loss_sum = 0
		n_sum = 0
		fpr = dict()
		tpr = dict()
		roc_auc = dict()
		outs = torch.Tensor([]).to(device)
		labels = torch.Tensor([]).long().to(device)
		for q, batch in enumerate(test_loader, 1):
			data, label = batch[0].to(device), batch[1].to(device)
			out = F.softmax(model(data), dim=-1)
			outs = torch.cat((outs, out), dim = 0)
			labels = torch.cat((labels, label), dim = 0)
			loss = Loss(out, label)
			_, pred = out.max(-1)
			acc = pred.eq(label).float().mean().item()
			acc_sum += acc*data.size(0)
			loss_sum += loss.item()*data.size(0)
			n_sum += data.size(0)
		for i in range(3): #3 = number of classes, grades 1, 2, and 3 
			#fpr[i], tpr[i], _ = roc_curve(labeltest.to(device), outs)
			fpr[i], tpr[i], _ = roc_curve(label_binarize(labels.cpu().numpy(), classes=[0,1,2])[:,i], outs.cpu().numpy()[:,i])
			roc_auc[i] = auc(fpr[i], tpr[i])
	print(f'acc {acc_sum/n_sum:.3f}; loss {loss_sum/n_sum:.4f}')
	print(f'AUC class 1 {roc_auc[0]}; AUC class 2 {roc_auc[1]}; AUC class 3 {roc_auc[2]}')
	plotAUC(fpr, tpr, roc_auc)

eval()
