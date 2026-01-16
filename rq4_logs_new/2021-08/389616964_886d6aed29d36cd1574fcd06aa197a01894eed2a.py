# -*- coding: utf-8 -*-
"""
Created on Sat Aug 14 18:27:04 2021

@author: 10983
"""
import torch
import time
import math

torch.cuda.manual_seed_all(123)#设置随机数种子，使每一次初始化数值都相同

def train(model,min_loss,trainloader,validloader,start,trainset,validset):
    total_loss=0
    gamma=0.1
    optimizer=torch.optim.Adam(model.parameters(),lr=0.1)
    scheduler=torch.optim.lr_scheduler.ExponentialLR(optimizer,gamma, last_epoch=-1)
    criterion=torch.nn.CrossEntropyLoss()
    acc_list=[]
    #循环100轮
    for epoch in range(1,101): 
        for i,(inputs,target) in enumerate(trainloader,1):
            optimizer.zero_grad()
            output=model.forward(inputs)
            loss=criterion(output,target)
            loss.backward()
            optimizer.step()
            scheduler.step()
        
            total_loss+=loss.item()
            if i%500==0:
                print(f'[{time_since(start)}] Epoch {epoch}',end='')
                print(f'[{i * len(inputs)}/{len(trainset)}]',end='')
                print(f'loss={total_loss/(i*len(inputs))}')

        print('train网络的参数：')
        i=0
        for parameters in model.parameters():
             print(parameters)
             i+=1
             if i==4:
                 break
        
        '''
        #如果出现损失值的最新低值，则保存网络模型
        if (total_loss/len(trainset))<min_loss:
            print('**********************')
            min_loss=total_loss/len(trainset) #更新值
            torch.save(classifier.state_dict(),'C:/Users/10983/py入门/GRUClassifier/net_params.pkl')
        '''
        
        acc=valid(model,validloader,criterion,validset,start,epoch)
        acc_list.append(acc)
    return acc_list

def valid(model,validloader,loss_func,validset,start,epoch):
    correct,test_total_loss=0,0
    total=len(validset)
    print('evaluating trained model...')
    #model.load_state_dict(torch.load('C:/Users/10983/py入门/GRUClassifier/net_params.pkl'))
    
    print()
    print('test网络的参数：')
    i=0
    for parameters in model.parameters():
        print(parameters)
        i+=1
        if i==4:
            break
    with torch.no_grad():#测试不需要求梯度
        for i,(inputs,target) in enumerate(validloader,1):
            output=model.forward(inputs)
            test_loss=loss_func(output,target)
            pred=output.max(dim=1,keepdim=True)[1]
            correct+=pred.eq(target.view_as(pred)).sum().item()
            
            test_total_loss+=test_loss.item()
            if i%100==0:
                print(f'[{time_since(start)}] Epoch {epoch}',end='')
                print(f'[{i * len(inputs)}/{len(validset)}]',end='')
                print(f'test_loss={test_total_loss/(i*len(inputs))}')
           
        percent='%.2f' % (100 * correct /total)
        print(f'Test set: Accuracy {correct}/{total} {percent} %')
        
    return correct / total  

def time_since(since):
    s=time.time()-since
    m=math.floor(s/60)
    s-=m*60
    return '%dm %ds'% (m,s)




















