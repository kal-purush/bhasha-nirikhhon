import torch
import torchvision
import torchvision.transforms as transforms
import argparse
import numpy as np
from torch.autograd import Variable
from experiment import ex
from model import load_model
from utils import post_config_hook

from modules import LogisticRegression
from modules.simclr import simclrnet
from modules.transformations import TransformsSimCLR

from cifar import CIFAR10, CIFAR100
from coteachingloss import loss_coteaching
import torch.nn.functional as F
import numpy as np
import shutil
import copy

'''
def train(args, loader, simclr_model, criterion, optimizer):
    loss_epoch = 0
    accuracy_epoch = 0
    clean_accuracy = 0
    for step, (x, y,cleany,_) in enumerate(loader):
   
        optimizer.zero_grad()

        x = x.to(args.device)
        y = y.to(args.device)
        cleany = cleany.to(args.device)

        output = simclr_model(x)
        loss = criterion(output, y)

        predicted = output.argmax(1)
        acc = (predicted == y).sum().item() / y.size(0)
        cleanacc =(predicted == cleany).sum().item() / cleany.size(0)
        accuracy_epoch += acc
        clean_accuracy += cleanacc

        loss.backward()
        optimizer.step()

        loss_epoch += loss.item()
        # if step % 100 == 0:
        #     print(
        #         f"Step [{step}/{len(loader)}]\t Loss: {loss.item()}\t Accuracy: {acc}"
        #     )

    return loss_epoch, accuracy_epoch,clean_accuracy


def test(args, loader, simclr_model, criterion, optimizer):
    loss_epoch = 0
    accuracy_epoch = 0
    simclr_model.eval()
    for step, (x, y,_,_) in enumerate(loader):
        simclr_model.zero_grad()

        x = x.to(args.device)
        y = y.to(args.device)

        output = simclr_model(x)
        loss = criterion(output, y)

        predicted = output.argmax(1)
        acc = (predicted == y).sum().item() / y.size(0)
        accuracy_epoch += acc

        loss_epoch += loss.item()

    return loss_epoch, accuracy_epoch
    
#label and unlabel stage
labeldata=[]
unlabeldata=[]
accuracy_epoch=0
clean_accuracy=0
for step, (x, y,cleany,_) in enumerate(train_loader):

    x = x.to(args.device)
    y = y.to(args.device)
    cleany = cleany.to(args.device)

    output = simclr_model(x)

    predicted = output.argmax(1)
    for i in range(len(predicted)):
        if predicted[i]==y[i]:
            labeldata.append((x,y))
        else:
            unlabeldata.append(x)
    acc = (predicted == y).sum().item() / y.size(0)
    cleanacc =(predicted == cleany).sum().item() / cleany.size(0)
    accuracy_epoch += acc
    clean_accuracy += cleanacc
print("\t Accuracy: {accuracy_epoch / len(train_loader)}\t Clean_accuracy: {clean_accuracy / len(train_loader)}")
print('*'*30+'labeled data length'+'*'*30)
print(len(labeldata))
print('*'*30+'unlabeled data length'+'*'*30)
print(len(unlabeldata))

'''


# Train the Model
def train(args,train_loader,epoch, model1, optimizer1, model2, optimizer2,rate_schedule,noise_or_not):
    
    pure_ratio_list=[]
    pure_ratio_1_list=[]
    pure_ratio_2_list=[]
    
    train_total=0
    train_correct=0 
    train_total2=0
    train_correct2=0 
  
    for step, (images, labels,cleanlabels,indexes) in enumerate(train_loader):
        ind=indexes.cpu().numpy().transpose()
        if step>args.num_iter_per_epoch:
            break
        
        images = Variable(images).to(args.device)
        labels = Variable(labels).to(args.device)
        
        # Forward + Backward + Optimize
        logits1 = model1(images)
        pred1 = logits1.argmax(1)
        
        train_total=train_total+len(images)
        train_correct=train_correct+ (pred1 == labels).sum().item() 

        logits2 = model2(images)
        pred2 = logits2.argmax(1)
        train_total2=train_total2+len(images)
        train_correct2=train_correct2+ (pred2 == labels).sum().item() 
        loss_1, loss_2, pure_ratio_1, pure_ratio_2 = loss_coteaching(logits1, logits2, labels, rate_schedule[epoch], ind, noise_or_not)
        
        pure_ratio_1_list.append(100*pure_ratio_1)
        pure_ratio_2_list.append(100*pure_ratio_2)
        
        optimizer1.zero_grad()
        loss_1.backward()
        optimizer1.step()
        optimizer2.zero_grad()
        loss_2.backward()
        optimizer2.step()
        train_acc1=float(train_correct)/float(train_total)
        train_acc2=float(train_correct2)/float(train_total2)
        if (step+1) % 50 == 0:
            print ('Epoch [%d/%d], Iter [%d/%d] , rate %.4f,Training Accuracy1: %.4F, Training Accuracy2: %.4f, Loss1: %.4f, Loss2: %.4f, Pure Ratio1: %.4f, Pure Ratio2 %.4f' 
                  %(epoch+1, args.n_epoch, step+1, len(train_loader),rate_schedule[epoch],train_acc1,train_acc2, loss_1.item(), loss_2.item(), np.sum(pure_ratio_1_list)/len(pure_ratio_1_list), np.sum(pure_ratio_2_list)/len(pure_ratio_2_list)))

    train_acc1=float(train_correct)/float(train_total)
    train_acc2=float(train_correct2)/float(train_total2)
    return train_acc1, train_acc2, pure_ratio_1_list, pure_ratio_2_list

# Evaluate the Model
def evaluate(args,test_loader, model1, model2):
 
    model1.eval()    # Change model to 'eval' mode.
    correct1 = 0
    total1 = 0
    for images, labels, cleanlabels, index in test_loader:
        images = Variable(images).to(args.device)
        labels = Variable(labels).to(args.device)
        logits1 = model1(images)
        outputs1 = F.softmax(logits1, dim=1)
        pred1 = outputs1.argmax(1)
        total1 += labels.size(0)
        correct1 = (pred1 == labels).sum().item() 

    model2.eval()    # Change model to 'eval' mode 
    correct2 = 0
    total2 = 0
    for images, labels, cleanlabels, index in test_loader:
        images = images.to(args.device)
        labels = labels.to(args.device)
        logits2 = model2(images)
        outputs2 = F.softmax(logits2, dim=1)
        pred2 = outputs2.argmax(1)
        total2 += labels.size(0)
        correct2 = (pred2 == labels).sum().item() 
 
    acc1 = 100*float(correct1)/float(total1)
    acc2 = 100*float(correct2)/float(total2)
    return acc1, acc2




@ex.automain
def main(_run, _log):
    args = argparse.Namespace(**_run.config)
    args = post_config_hook(args, _run)

    args.device = torch.device("cuda:7" )
    if args.forget_rate is None:
        forget_rate=args.noise_rate
    else:
        forget_rate=args.forget_rate
        
    # Adjust learning rate and betas for Adam Optimizer
    mom1 = 0.9
    mom2 = 0.1
    alpha_plan = [args.lr] * args.n_epoch
    beta1_plan = [mom1] * args.n_epoch
    for i in range(args.epoch_decay_start, args.n_epoch):
        alpha_plan[i] = float(args.n_epoch - i) / (args.n_epoch - args.epoch_decay_start) * args.lr
        beta1_plan[i] = mom2
    
    def adjust_learning_rate(optimizer, epoch):
        for param_group in optimizer.param_groups:
            param_group['lr']=alpha_plan[epoch]
            param_group['betas']=(beta1_plan[epoch], 0.999) # Only change beta1
            
    # define drop rate schedule
    '''
    rate_schedule=[]
    tmp=forget_rate
    for i in range(args.n_epoch):
        rate_schedule.append(tmp)
        tmp=tmp*1.05
    rate_schedule=np.array(rate_schedule)
    
    rate_schedule = np.ones(args.n_epoch)*(forget_rate**args.exponent)
    rate_schedule[:args.num_gradual] = np.linspace(forget_rate, forget_rate**args.exponent, args.num_gradual)
    '''
    
    
    rate_schedule = np.ones(args.n_epoch)*(forget_rate**args.exponent)
    rate_schedule[:args.num_gradual] = np.linspace(0, forget_rate**args.exponent, args.num_gradual)
    


    # with torch.autograd.set_detect_anomaly(True):
    root = "./datasets"

    if args.dataset == "STL10":
        train_dataset = torchvision.datasets.STL10(
            root,
            split="train",
            download=True,
            transform=TransformsSimCLR(size=224).test_transform,
        )
        test_dataset = torchvision.datasets.STL10(
            root,
            split="test",
            download=True,
            transform=TransformsSimCLR(size=224).test_transform,
        )
    elif args.dataset == "CIFAR10":
        train_dataset = CIFAR10(root='./datasets/',
                                download=True,
                                train=True,
                                transform=transforms.ToTensor(),
                                noise_type=args.noise_type,
                                noise_rate=args.noise_rate
                           )

        test_dataset = CIFAR10(root='./datasets/',
                                download=True,
                                train=False,
                                transform=transforms.ToTensor(),
                                noise_type=args.noise_type,
                                noise_rate=args.noise_rate
                          )
    elif args.dataset == "CIFAR100":
        train_dataset = CIFAR100(root='./datasets/',
                                download=True,
                                train=True,
                                transform=transforms.ToTensor(),
                                noise_type=args.noise_type,
                                noise_rate=args.noise_rate
                           )

        test_dataset = CIFAR100(root='./datasets/',
                                download=True,
                                train=False,
                                transform=transforms.ToTensor(),
                                noise_type=args.noise_type,
                                noise_rate=args.noise_rate
                          )
    else:
        raise NotImplementedError


    noise_or_not = train_dataset.noise_or_not

    train_loader = torch.utils.data.DataLoader(
        train_dataset,
        batch_size=args.logistic_batch_size,
        shuffle=True,
        drop_last=True,
        num_workers=args.workers,
    )

    test_loader = torch.utils.data.DataLoader(
        test_dataset,
        batch_size=args.logistic_batch_size,
        shuffle=False,
        drop_last=True,
        num_workers=args.workers,
    )



    with torch.autograd.set_detect_anomaly(True):
        simclr_model, _, _ = load_model(args, train_loader, reload_model=True)
        simclr_model2 = copy.deepcopy(simclr_model)

        in_feature=simclr_model.n_features
        n_classes = 10
        simclr_model1=simclrnet(args,simclr_model.encoder,n_classes,in_feature).to(args.device)
        # simclr_model2=simclrnet(args,simclr_model.encoder,n_classes,in_feature).to(args.device)
        simclr_model2=simclrnet(args,simclr_model2.encoder,n_classes,in_feature).to(args.device)

        simclr_model1.eval()
        simclr_model2.eval()
        
        optimizer1 = torch.optim.Adam(simclr_model1.parameters(), lr=args.lr)
        optimizer2 = torch.optim.Adam(simclr_model2.parameters(), lr=args.lr)
        #criterion = torch.nn.CrossEntropyLoss()
        mean_pure_ratio1=0
        mean_pure_ratio2=0
        
        epoch=0
        train_acc1=0
        train_acc2=0
        print('===> Evaluate first')
        # evaluate models with random weights
        print(args.device)
        test_acc1, test_acc2=evaluate(args,test_loader, simclr_model1, simclr_model2)
        print('Epoch [%d/%d] Test Accuracy on the %s test images: Model1 %.4f %% Model2 %.4f %% Pure Ratio1 %.4f %% Pure Ratio2 %.4f %%' % (epoch+1, args.n_epoch, len(test_dataset), test_acc1, test_acc2, mean_pure_ratio1, mean_pure_ratio2))
        
        print('===> Training stage')
        # training stage
        for epoch in range(1, args.n_epoch):
            # train models
            simclr_model1.train()
            adjust_learning_rate(optimizer1, epoch)
            simclr_model2.train()
            adjust_learning_rate(optimizer2, epoch)
            train_acc1, train_acc2, pure_ratio_1_list, pure_ratio_2_list=train(args,train_loader, epoch, simclr_model1, optimizer1, simclr_model2, optimizer2,rate_schedule,noise_or_not)
            # evaluate models
            test_acc1, test_acc2=evaluate(args,test_loader, simclr_model1, simclr_model2)
            # save results
            mean_pure_ratio1 = sum(pure_ratio_1_list)/len(pure_ratio_1_list)
            mean_pure_ratio2 = sum(pure_ratio_2_list)/len(pure_ratio_2_list)
            print('Epoch [%d/%d] Test Accuracy on the %s test images: Model1 %.4f %% Model2 %.4f %%, Pure Ratio 1 %.4f %%, Pure Ratio 2 %.4f %%' % (epoch+1, args.n_epoch, len(test_dataset), test_acc1, test_acc2, mean_pure_ratio1, mean_pure_ratio2))
            
         
            
            
