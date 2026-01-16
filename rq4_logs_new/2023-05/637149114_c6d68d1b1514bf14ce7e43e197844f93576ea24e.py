'''
在整个Pytorch框架中, 所有的神经网络本质上都是一个autograd package(自动求导工具包)
autograd package提供了一个对Tensors上所有的操作进行自动微分的功能.
torch.Tensor是整个package中的核心类, 如果将属性.requires_grad设置为True, 它将追踪在这个类上定义的所有操作. 当代码要进行反向传播的时候, 直接调用.backward()就可以自动计算所有的梯度. 在这个Tensor上的所有梯度将被累加进属性.grad中.
如果想终止一个Tensor在计算图中的追踪回溯, 只需要执行.detach()就可以将该Tensor从计算图中撤下, 在未来的回溯计算中也不会再计算该Tensor.
除了.detach(), 如果想终止对计算图的回溯, 也就是不再进行方向传播求导数的过程, 也可以采用代码块的方式with torch.no_grad():, 这种方式非常适用于对模型进行预测的时候, 因为预测阶段不再需要对梯度进行计算.

关于torch.Function:
Function类是和Tensor类同等重要的一个核心类, 它和Tensor共同构建了一个完整的类, 每一个Tensor拥有一个.grad_fn属性, 代表引用了哪个具体的Function创建了该Tensor.
如果某个张量Tensor是用户自定义的, 则其对应的grad_fn is None
'''
import torch

x1 = torch.ones(3, 3)
print(x1)
# 如果某个张量Tensor是用户自定义的, 则其对应的grad_fn is None
x = torch.ones(2, 2, requires_grad=True)
print(x)

y = x + 2
print(y)

print(x.grad_fn)
print(y.grad_fn)
print("++++++++++++++++++++++++++++++++++++++++")

z = y * y * 3
out = z.mean()
print(z)
print(out)
print("++++++++++++++++++++++++++++++++++++++++")
'''
关于方法.requires_grad_(): 该方法可以原地改变Tensor的属性.requires_grad的值. 如果没有主动设定默认为False.
'''
a = torch.randn(2, 2)
a = ((a * 3) / (a - 1))
print(a.requires_grad)
a.requires_grad_(True)
print(a.requires_grad)
b = (a * a).sum()
print(b.grad_fn)
print("++++++++++++++++++++++++++++++++++++++++")
# 当代码要进行反向传播的时候, 直接调用.backward()就可以自动计算所有的梯度. 在这个Tensor上的所有梯度将被累加进属性.grad中.
out.backward()
print(x.grad)

# x的requires_grad = true,则x平方的requires_grad也是true

print(x.requires_grad)
print((x ** 2).requires_grad)

with torch.no_grad():
    print((x ** 2).requires_grad)
# 可以通过.detach()获得一个新的Tensor, 拥有相同的内容但不需要自动求导.
print(x.requires_grad)
y = x.detach()
print(y.requires_grad)
# 检测x y 是否拥有相同的内容(值比较矩阵)
print(x.eq(y).all())
'''
关于torch.nn:
使用Pytorch来构建神经网络, 主要的工具都在torch.nn包中.
nn依赖于autograd来定义模型, 并对其自动求导.

构建神经网络的典型流程:
定义一个拥有可学习参数的神经网络
遍历训练数据集
处理输入数据使其流经神经网络
计算损失值
将网络参数的梯度进行反向传播
以一定的规则更新网络的权重

激活函数Relu，在神经网络中的作用是：通过加权的输入进行非线性组合产生非线性决策边界 简单的来说就是增加非线性作用。
在深层卷积神经网络中使用激活函数同样也是增加非线性，主要是为了解决sigmoid函数带来的梯度消失问题。

在PyTorch中对于不能整除的状况默认均为向下取整，可以选择向上取整
'''
import torch
# 导入若干工具包
import torch
import torch.nn as nn
import torch.nn.functional as F


# 定义一个简单的网络类
class Net(nn.Module):

    def __init__(self):
        super(Net, self).__init__()
        # 定义第一层卷积神经网络, 输入通道维度=1, 输出通道维度=6, 卷积核大小3*3
        self.conv1 = nn.Conv2d(1, 6, 3)
        # 定义第二层卷积神经网络, 输入通道维度=6, 输出通道维度=16, 卷积核大小3*3
        self.conv2 = nn.Conv2d(6, 16, 3)
        # 定义三层全连接网络
        self.fc1 = nn.Linear(16 * 6 * 6, 120)
        self.fc2 = nn.Linear(120, 84)
        self.fc3 = nn.Linear(84, 10)

    def forward(self, x):
        # 在(2, 2)的池化窗口下执行最大池化操作
        x = F.max_pool2d(F.relu(self.conv1(x)), (2, 2))
        x = F.max_pool2d(F.relu(self.conv2(x)), 2)
        x = x.view(-1, self.num_flat_features(x))
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))
        x = self.fc3(x)
        return x

    def num_flat_features(self, x):
        # 计算size, 除了第0个维度上的batch_size
        size = x.size()[1:]
        num_features = 1
        for s in size:
            num_features *= s
        return num_features


net = Net()
print(net)

params = list(net.parameters())
print(len(params))
print(params[0].size())

'''
orch.randn[8, 3, 244, 244]，[batch, channel, height, width]，表示batch_size=8， 3通道（灰度图像为1），图片尺寸：224x224
如果给的是torch.randn[1, 1, 32, 32]表示batch_size=1， 1通道（灰度图像），图片尺寸：32x32
'''
input = torch.randn(1, 1, 32, 32)
print("+++++++++++++++++")
print(input)
print(input.size())
print("+++++++++++++++++")
out = net(input)
print(out)
print(out.size())
'''
torch.nn构建的神经网络只支持mini-batches的输入, 不支持单一样本的输入.
比如: nn.Conv2d 需要一个4D Tensor, 形状为(nSamples, nChannels, Height, Width). 如果你的输入只有单一样本形式,
 则需要执行input.unsqueeze(0), 主动将3D Tensor扩充成4D Tensor.
'''
'''
损失函数的输入是一个输入的pair: (output, target), 然后计算出一个数值来评估output和target之间的差距大小.
在torch.nn中有若干不同的损失函数可供使用, 比如nn.MSELoss就是通过计算均方差损失来评估输入和目标值之间的差距.
'''
output = net(input)
target = torch.randn(10)
print("+++++++++++++++++")
print(target)
print(target.size())
print("+++++++++++++++++")
# 改变target的形状为二维张量, 为了和output匹配
target = target.view(1, -1)
print("+++++++++++++++++")
print(target)
print(target.size())
print("+++++++++++++++++")
criterion = nn.MSELoss()

loss = criterion(output, target)
print(loss)
'''
关于方向传播的链条: 如果我们跟踪loss反向传播的方向, 使用.grad_fn属性打印, 将可以看到一张完整的计算图如下:
input -> conv2d -> relu -> maxpool2d -> conv2d -> relu -> maxpool2d
      -> view -> linear -> relu -> linear -> relu -> linear
      -> MSELoss
      -> loss
'''
'''
当调用loss.backward()时, 整张计算图将对loss进行自动求导, 
所有属性requires_grad=True的Tensors都将参与梯度求导的运算, 并将梯度累加到Tensors中的.grad属性中.
'''
print(loss.grad_fn)  # MSELoss
print(loss.grad_fn.next_functions[0][0])  # Linear
print(loss.grad_fn.next_functions[0][0].next_functions[0][0])  # ReLU

'''
反向传播(backpropagation)
在Pytorch中执行反向传播非常简便, 全部的操作就是loss.backward().
在执行反向传播之前, 要先将梯度清零, 否则梯度会在不同的批次数据之间被累加.
'''
# Pytorch中执行梯度清零的代码
net.zero_grad()

print('conv1.bias.grad before backward')
print(net.conv1.bias.grad)

# Pytorch中执行反向传播的代码
loss.backward()

print('conv1.bias.grad after backward')
print(net.conv1.bias.grad)


# 首先导入优化器的包, optim中包含若干常用的优化算法, 比如SGD, Adam等
import torch.optim as optim

# 通过optim创建优化器对象
optimizer = optim.SGD(net.parameters(), lr=0.01)

# 将优化器执行梯度清零的操作
optimizer.zero_grad()

output = net(input)
loss = criterion(output, target)

# 对损失值执行反向传播的操作
loss.backward()
# 参数的更新通过一行标准代码来执行
optimizer.step()

print("hello world")
import torch
import torchvision
import torchvision.transforms as transforms
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim

# 使用torchvision下载CIFAR10数据集
'''
下载数据集并对图片进行调整,
因为torchvision数据集的输出是PILImage格式, 数据域在[0, 1]. 我们将其转换为标准数据域[-1, 1]的张量格式.
'''
# 利用transforms.compose进行转换，转换为tensor类型
# 下面是标准代码，定义数据转换器
transform = transforms.Compose(
    [transforms.ToTensor(),
     transforms.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))])

trainset = torchvision.datasets.CIFAR10(root='./data', train=True,
                                        download=True, transform=transform)
# num_workers=2是两个线程，多个线程加速读取数据速度
trainloader = torch.utils.data.DataLoader(trainset, batch_size=4,
                                          shuffle=True)

testset = torchvision.datasets.CIFAR10(root='./data', train=False,
                                       download=True, transform=transform)
# 测试时不需要打乱，所以shuffle=False
testloader = torch.utils.data.DataLoader(testset, batch_size=4,
                                         shuffle=False)

classes = ('plane', 'car', 'bird', 'cat',
           'deer', 'dog', 'frog', 'horse', 'ship', 'truck')


# 仿照7.1节中的类来构造此处的类, 唯一的区别是此处采用3通道3-channel

class Net(nn.Module):
    def __init__(self):
        super(Net, self).__init__()
        self.conv1 = nn.Conv2d(3, 6, 5)
        self.pool = nn.MaxPool2d(2, 2)
        self.conv2 = nn.Conv2d(6, 16, 5)
        self.fc1 = nn.Linear(16 * 5 * 5, 120)
        self.fc2 = nn.Linear(120, 84)
        self.fc3 = nn.Linear(84, 10)

    def forward(self, x):
        x = self.pool(F.relu(self.conv1(x)))
        x = self.pool(F.relu(self.conv2(x)))
        x = x.view(-1, 16 * 5 * 5)
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))
        x = self.fc3(x)
        return x


net = Net()
print(net)

'''
Momentum的作用？
主要是在训练网络时，最开始会对网络进行权值初始化，但是这个初始化不可能是最合适的；
因此可能就会出现损失函数在训练的过程中出现局部最小值的情况，而没有达到全局最优的状态。
momentum的出现可以在一定程度上解决这个问题。动量来源于物理学，当momentum越大时，
转换为势能的能量就越大，就越有可能摆脱局部凹区域，从而进入全局凹区域。momentum主要是用于权值优化。
'''

# 采用交叉熵损失函数和随机梯度下降优化器.
criterion = nn.CrossEntropyLoss()
optimizer = optim.SGD(net.parameters(), lr=0.001, momentum=0.9)

for epoch in range(2):  # loop over the dataset multiple times

    running_loss = 0.0
    for i, data in enumerate(trainloader, 0):
        # data中包含输入图像张量inputs, 标签张量labels
        inputs, labels = data

        # 首先将优化器梯度归零
        optimizer.zero_grad()

        # 输入图像张量进网络, 得到输出张量outputs
        outputs = net(inputs)

        # 利用网络的输出outputs和标签labels计算损失值
        loss = criterion(outputs, labels)

        # 反向传播+参数更新, 是标准代码的标准流程
        loss.backward()
        optimizer.step()

        # 打印轮次和损失值
        running_loss += loss.item()
        if (i + 1) % 2000 == 0:
            print('[%d, %5d] loss: %.3f' %
                  (epoch + 1, i + 1, running_loss / 2000))
            running_loss = 0.0

print('Finished Training')

# 首先设定模型的保存路径
PATH = './cifar_net.pth'
# 保存模型的状态字典
torch.save(net.state_dict(), PATH)

# 先拿出四张图片进行简单的测试
# 先取出4张图片
dataiter = iter(testloader)
# 两种利用迭代器的方法
# images, labels = next(dataiter)
images, labels = dataiter.__next__()
# 首先实例化模型的类对象
net = Net()
# 加载训练阶段保存好的模型的状态字典
net.load_state_dict(torch.load(PATH))
# 利用模型对图片进行预测
outputs = net(images)
# 共有10个类别, 采用模型计算出的概率最大的作为预测的类别
# dim=0表示计算每列的最大值，dim=1表示每行的最大值
_, predicted = torch.max(outputs, 1)
# 打印预测标签的结果
print('Predicted: ', ' '.join('%5s' % classes[predicted[j]] for j in range(4)))

# 在全部测试集上运行
correct = 0
total = 0
with torch.no_grad():
    for data in testloader:
        # data中有四个数据
        images, labels = data
        '''
        torch.Size括号中有几个数字就是几维
        例如：第一层（最外层）中括号里面包含了两个中括号（以逗号进行分割），这就是（2，3，4）中的2
        第二层中括号里面包含了三个中括号（以逗号进行分割），这就是（2，3，4）中的3
        第三层中括号里面包含了四个数（以逗号进行分割），这就是（2，3，4）中的4
        '''
        print("++++++++++++++")
        print(labels)
        print(type(labels))
        print(labels.size)
        print(labels.size())
        # 范围只能是-1 到0
        # 具体原因看main.py
        print(labels.size(-1))
        print(labels.size(0))
        print("++++++++++++++")
        outputs = net(images)
        # dim=0表示计算每列的最大值，dim=1表示每行的最大值
        # 返回值有两个，取后者。取返回最大值所在的索引
        _, predicted = torch.max(outputs.data, 1)
        # labels.size(0)
        total += labels.size(0)
        # predicted和labels都是一行四列的值。sum把其中的值为TRUE的加起来
        correct += (predicted == labels).sum().item()
# %% 字符%
print('Accuracy of the network on the 10000 test images: %d %%' % (
        100 * correct / total))

print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
# 为了更加细致的看一下模型在哪些类别上表现更好, 在哪些类别上表现更差, 我们分类别的进行准确率计算.
class_correct = list(0. for i in range(10))
# class_correct中有10个0.0
class_total = list(0. for i in range(10))
# class_total中有10个0.0
with torch.no_grad():
    for data in testloader:
        images, labels = data
        outputs = net(images)
        _, predicted = torch.max(outputs, 1)
        print(predicted)
        print(predicted.size())
        print(predicted == labels)
        print((predicted == labels).size())
        # 把.squeeze()删除也能运行
        # https://zhuanlan.zhihu.com/p/368920094
        c = (predicted == labels).squeeze()
        print(c)
        print(c.size())
        for i in range(4):
            label = labels[i]
            class_correct[label] += c[i].item()
            class_total[label] += 1


for i in range(10):
    print('Accuracy of %5s : %2d %%' % (
        classes[i], 100 * class_correct[i] / class_total[i]))
import jieba
import hanlp
from hanlp.utils.lang.en.english_tokenizer import tokenize_english
import jieba.posseg as pseg

content = "工信处女干事每月经过下属科室都要亲口交代24口交换机等技术性器件的安装工作"
cut = jieba.cut(content, cut_all=False)
print(cut)
lcut = jieba.lcut(content, cut_all=False)
print(lcut)
# 全模式把所有能分词的都分割出来
jieba_lcut = jieba.lcut(content, cut_all=True)
print(jieba_lcut)

content = "煩惱即是菩提，我暫且不提"
l = jieba.lcut(content, cut_all=False)
print(l)

lcut1 = jieba.lcut("八一双鹿更名为八一南昌篮球队！")
print(lcut1)

jieba.load_userdict("./user_dict.txt")
lcut1 = jieba.lcut("八一双鹿更名为八一南昌篮球队！")
print(lcut1)
#
# tokenizer = hanlp.load('CTB6_CONVSEG')
# tokenizer("工信处女干事每月经过下属科室都要亲口交代24口交换机等技术性器件的安装工作")

tokenizer = tokenize_english
list_list = tokenizer('Mr. Hankcs bought hankcs.com for 1.5 thousand dollars.')
print(list_list)

# recognizer = hanlp.load(hanlp.pretrained.ner.MSRA_NER_BERT_BASE_ZH)
# recognizer1 = recognizer(list('上海华安工业（集团）公司董事长谭旭光和秘书张晚霞来到美国纽约现代艺术博物馆参观。'))
# print(recognizer1)


# recognizer = hanlp.load(hanlp.pretrained.ner.CONLL03_NER_BERT_BASE_UNCASED_EN))
# recognizer1 = recognizer(["President", "Obama", "is", "speaking", "at", "the", "White", "House"])
# print(recognizer1)


pseg_lcut = pseg.lcut("我爱北京天安门")
print(pseg_lcut)
# 导入用于对象保存与加载的joblib
import joblib
# 导入keras中的词汇映射器Tokenizer
from keras.preprocessing.text import Tokenizer

# 假定vocab为语料集所有不同词汇集合
vocab = {"周杰伦", "陈奕迅", "王力宏", "李宗盛", "吴亦凡", "鹿晗"}
# 实例化一个词汇映射器对象
t = Tokenizer(num_words=None, char_level=False)
# 使用映射器拟合现有文本数据
t.fit_on_texts(vocab)

for token in vocab:
    zero_list = [0] * len(vocab)
    # 使用映射器转化现有文本数据, 每个词汇对应从1开始的自然数
    # 返回样式如: [[2]], 取出其中的数字需要使用[0][0]
    token_index = t.texts_to_sequences([token])[0][0] - 1
    zero_list[token_index] = 1
    print(token, "的one-hot编码为:", zero_list)

# 使用joblib工具保存映射器, 以便之后使用
tokenizer_path = "./Tokenizer"
joblib.dump(t, tokenizer_path)
import fasttext

model = fasttext.train_unsupervised('data1/enwik9')