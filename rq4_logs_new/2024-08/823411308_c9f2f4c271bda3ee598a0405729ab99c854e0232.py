# nvidia-smi 显卡信息powershell
import torch
from torch import nn

# 1. 检查可用的 GPU 设备
def try_gpu(i=0):
    """如果存在，则返回 gpu(i)，否则返回 cpu()"""
    if torch.cuda.device_count() >= i + 1:
        return torch.device(f'cuda:{i}')
    return torch.device('cpu')

def try_all_gpus():
    """返回所有可用的 GPU，如果没有 GPU，则返回 [cpu(),]"""
    devices = [torch.device(f'cuda:{i}') for i in range(torch.cuda.device_count())]
    return devices if devices else [torch.device('cpu')]

# 打印可用的设备
print("设备:", try_gpu(), try_gpu(10), try_all_gpus())

# 2. 张量与 GPU 的操作
# 创建张量并指定设备
X = torch.ones(2, 3, device=try_gpu())
print("X 张量:", X)

# 创建另一个张量并指定不同的 GPU 设备
Y = torch.rand(2, 3, device=try_gpu(1))
print("Y 张量:", Y)

# # 张量间的操作需要在同一设备上
# # 将 X 张量复制到 GPU 1
# Z = X.cuda(1)
# print("X 张量 (GPU 0):", X)
# print("Z 张量 (GPU 1):", Z)
#
# # 在相同的 GPU 上进行张量操作
# result = Y + Z
# print("张量加法结果:", result)
#
# # 确保 Z 张量已经在 GPU 1 上
# print("Z 张量是否仍然在 GPU 1 上:", Z.cuda(1) is Z)

# 3. 神经网络与 GPU
# 定义一个简单的神经网络
net = nn.Sequential(nn.Linear(3, 1))

# 将模型参数放到 GPU 上
net = net.to(device=try_gpu())
print("网络参数设备:", net[0].weight.data.device)

# 使用模型进行预测
X = torch.ones(2, 3, device=try_gpu())
output = net(X)
print("模型输出:", output)
import torch
from torch import nn

# 定义一个具有单隐藏层的多层感知机（MLP）模型
net = nn.Sequential(
    nn.Linear(4, 8),  # 第一层，全连接层，将输入的4个特征映射到8个特征
    nn.ReLU(),        # 激活函数ReLU
    nn.Linear(8, 1)   # 第二层，全连接层，将8个特征映射到1个输出
)

# 生成一个随机输入张量，大小为 (2, 4)
X = torch.rand(size=(2, 4))
print(net(X))  # 前向传播，通过模型得到输出

# 5.2.1 参数访问

# 访问第二个全连接层的参数
print("第二个全连接层的参数:", net[2].state_dict())

# 提取偏置参数，并查看其类型和数值
print("偏置参数类型:", type(net[2].bias))
print("偏置参数内容:", net[2].bias)
print("偏置参数数值:", net[2].bias.data)

# 查看权重梯度（未经过反向传播时梯度为None）
print("权重梯度状态:", net[2].weight.grad)

# 一次性访问所有参数
print("第一个全连接层的参数:", *[(name, param.shape) for name, param in net[0].named_parameters()])
print("所有层的参数:", *[(name, param.shape) for name, param in net.named_parameters()])

# 使用 state_dict 方法访问参数
print("第二个全连接层偏置的数值:", net.state_dict()['2.bias'].data)

# 从嵌套块收集参数

# 定义一个包含两个线性层的块
def block1():
    return nn.Sequential(
        nn.Linear(4, 8),
        nn.ReLU(),
        nn.Linear(8, 4),
        nn.ReLU()
    )

# 将多个块组合成更大的块
def block2():
    net = nn.Sequential()
    for i in range(4):
        net.add_module(f'block {i}', block1())  # 嵌套块
    return net

# 使用嵌套块创建新的网络
rgnet = nn.Sequential(block2(), nn.Linear(4, 1))
print(rgnet(X))  # 前向传播

# 查看嵌套网络的结构
print("嵌套网络的结构:", rgnet)

# 访问嵌套块的参数
print("嵌套块中第一个主要块的第二个子块的第一个层的偏置项:", rgnet[0][1][0].bias.data)

# 5.2.2 参数初始化

# 使用自定义初始化方法初始化模型参数
def init_normal(m):
    if type(m) == nn.Linear:
        nn.init.normal_(m.weight, mean=0, std=0.01)  # 正态分布初始化权重
        nn.init.zeros_(m.bias)                      # 将偏置初始化为0

net.apply(init_normal)  # 将初始化方法应用于网络
print("初始化后的权重和偏置:", net[0].weight.data[0], net[0].bias.data[0])

# 使用常数初始化
def init_constant(m):
    if type(m) == nn.Linear:
        nn.init.constant_(m.weight, 1)  # 将权重初始化为常数1
        nn.init.zeros_(m.bias)          # 将偏置初始化为0

net.apply(init_constant)
print("常数初始化后的权重和偏置:", net[0].weight.data[0], net[0].bias.data[0])

# 使用 Xavier 初始化和常数初始化
def init_xavier(m):
    if type(m) == nn.Linear:
        nn.init.xavier_uniform_(m.weight)  # Xavier 初始化

def init_42(m):
    if type(m) == nn.Linear:
        nn.init.constant_(m.weight, 42)  # 将权重初始化为常数42

net[0].apply(init_xavier)
net[2].apply(init_42)
print("Xavier 初始化后的权重:", net[0].weight.data[0])
print("常数42初始化后的权重:", net[2].weight.data)

# 自定义初始化方法
def my_init(m):
    if type(m) == nn.Linear:
        print("Init", *[(name, param.shape) for name, param in m.named_parameters()][0])
        nn.init.uniform_(m.weight, -10, 10)  # 均匀分布初始化权重
        m.weight.data *= m.weight.data.abs() >= 5  # 保留绝对值大于等于5的部分

net.apply(my_init)
print("自定义初始化后的权重:", net[0].weight[:2])

# 直接设置参数值
net[0].weight.data[:] += 1
net[0].weight.data[0, 0] = 42
print("手动设置后的权重:", net[0].weight.data[0])

# 5.2.3 参数绑定

# 创建一个共享的层
shared = nn.Linear(8, 8)
net = nn.Sequential(
    nn.Linear(4, 8),
    nn.ReLU(),
    shared,  # 使用共享的层
    nn.ReLU(),
    shared,  # 再次使用共享的层
    nn.ReLU(),
    nn.Linear(8, 1)
)

# 检查参数是否相同
print("检查共享层的权重是否相同:", net[2].weight.data[0] == net[4].weight.data[0])
net[2].weight.data[0, 0] = 100
print("修改共享层的权重后再检查:", net[2].weight.data[0] == net[4].weight.data[0])
import torch
import torch.nn.functional as F
from torch import nn

# 定义一个没有参数的自定义层 CenteredLayer
class CenteredLayer(nn.Module):
    def __init__(self):
        super().__init__()  # 调用父类的初始化函数

    def forward(self, X):
        """
        前向传播函数: 从输入 X 中减去均值。
        Args:
            X (Tensor): 输入的张量
        Returns:
            Tensor: 返回减去均值后的张量
        """
        return X - X.mean()

# 创建 CenteredLayer 实例并进行测试
layer = CenteredLayer()
output = layer(torch.FloatTensor([1, 2, 3, 4, 5]))
print("CenteredLayer输出:", output)

# 将 CenteredLayer 作为组件合并到更复杂的模型中
net = nn.Sequential(nn.Linear(8, 128), CenteredLayer())

# 测试新网络的输出均值是否为零
Y = net(torch.rand(4, 8))
print("新网络输出的均值:", Y.mean())

# 定义一个带参数的自定义层 MyLinear
class MyLinear(nn.Module):
    def __init__(self, in_units, units):
        """
        自定义全连接层的初始化函数。
        Args:
            in_units (int): 输入维度
            units (int): 输出维度
        """
        super().__init__()  # 调用父类的初始化函数
        # 初始化权重和偏置参数，使用均值为0，标准差为1的正态分布
        self.weight = nn.Parameter(torch.randn(in_units, units))
        self.bias = nn.Parameter(torch.randn(units,))

    def forward(self, X):
        """
        前向传播函数: 计算线性变换并应用ReLU激活函数。
        Args:
            X (Tensor): 输入的张量
        Returns:
            Tensor: 应用ReLU后的输出张量
        """
        # 计算线性变换：X @ W + b
        linear = torch.matmul(X, self.weight.data) + self.bias.data
        # 应用ReLU激活函数并返回
        return F.relu(linear)

# 实例化 MyLinear 类并访问其模型参数
linear = MyLinear(5, 3)
print("自定义层的权重参数:", linear.weight)

# 使用自定义层直接执行前向传播计算
output = linear(torch.rand(2, 5))
print("自定义层的输出:", output)

# 使用自定义层构建模型
net = nn.Sequential(MyLinear(64, 8), MyLinear(8, 1))
output = net(torch.rand(2, 64))
print("自定义网络的输出:", output)
import torch
from torch import nn
from torch.nn import functional as F

# 1. 加载和保存张量
# 定义一个简单的张量
x = torch.arange(4)
# 保存张量到文件 'x-file'
torch.save(x, 'x-file')

# 从文件 'x-file' 中加载张量数据
x2 = torch.load('x-file')
print("加载的张量 x2:", x2)  # 输出: tensor([0, 1, 2, 3])

# 保存一个张量列表到文件 'x-files'
y = torch.zeros(4)
torch.save([x, y], 'x-files')

# 从文件 'x-files' 中加载张量列表
x2, y2 = torch.load('x-files')
print("加载的张量列表 (x2, y2):", (x2, y2))  # 输出: (tensor([0, 1, 2, 3]), tensor([0., 0., 0., 0.]))

# 保存一个字典到文件 'mydict'
mydict = {'x': x, 'y': y}
torch.save(mydict, 'mydict')

# 从文件 'mydict' 中加载字典
mydict2 = torch.load('mydict')
print("加载的字典 mydict2:", mydict2)  # 输出: {'x': tensor([0, 1, 2, 3]), 'y': tensor([0., 0., 0., 0.])}


# 2. 加载和保存模型参数
# 定义一个简单的多层感知机(MLP)模型
class MLP(nn.Module):
    def __init__(self):
        super().__init__()
        # 定义隐藏层和输出层
        self.hidden = nn.Linear(20, 256)  # 隐藏层: 输入 20, 输出 256
        self.output = nn.Linear(256, 10)  # 输出层: 输入 256, 输出 10

    def forward(self, x):
        """
        前向传播函数: 通过隐藏层和输出层进行计算。
        Args:
            x (Tensor): 输入的张量
        Returns:
            Tensor: 网络的输出
        """
        # 使用ReLU激活函数进行计算
        return self.output(F.relu(self.hidden(x)))

# 创建MLP模型实例并生成一些随机输入数据
net = MLP()
X = torch.randn(size=(2, 20))  # 生成一个 2x20 的随机输入张量
Y = net(X)  # 使用模型进行前向传播计算
print("模型的输出 Y:", Y)

# 保存模型参数到文件 'mlp.params'
torch.save(net.state_dict(), 'mlp.params')

# 恢复模型：先创建一个相同结构的MLP模型实例
clone = MLP()

# 从文件 'mlp.params' 加载模型参数到克隆的模型实例中
clone.load_state_dict(torch.load('mlp.params'))
clone.eval()  # 将模型设置为评估模式

print("加载的模型 clone:", clone)

# 验证两个模型在相同输入下的输出是否相同
Y_clone = clone(X)
print("两个模型的输出是否相同:", Y_clone == Y)  # 逐元素比较输出