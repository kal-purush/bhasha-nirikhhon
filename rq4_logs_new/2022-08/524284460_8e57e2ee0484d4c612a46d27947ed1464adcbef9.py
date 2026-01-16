import numpy as np
import pandas as pd
import torch
from torch import nn
from sklearn import metrics
from torch.utils.data import DataLoader
from tqdm import tqdm
from sklearn.model_selection import KFold
from visdom import Visdom

# # visdom画图
viz = Visdom()
viz.line([[0.5, 0.5]], [0.], win='xuetangx loss', opts=dict(
    title='loss', legend=['train_loss', 'test_loss']))

viz.line([[0.5, 0.5, 0.5, 0.5]], [0.], win='xuetangx rmse&mae', opts=dict(
    title='rmse&mae', legend=['train_rmse', 'valid_rmse', 'train_mae', 'valid_mae']))



def evaluate(pred, label):
    # mae，平均绝对误差指的就是模型预测值 f(x) 与样本真实值 y 之间距离的平均值
    mae = metrics.mean_absolute_error(label, pred)
    # rmse，均方误差回归损失，为什么✖0.5？
    rmse = metrics.mean_squared_error(label, pred) ** 0.5
    return rmse, mae


# 训练集
def format_data(train_record, n_splits=3):
    # train_record对应train_data
    train = [[], [], []]  # 学生id，对应课程id, 对应的分数
    label = [[], [], []]  # 学生, 课程id，活跃度  (用来计算loss)
    # set具有去重的功能,stu_list是一个以学生列为索引的dataframe表格
    stu_list = set(train_record.index)
    # print(f'sss {train_record.index}')

    KF = KFold(n_splits=n_splits, shuffle=True)  # 3折交叉验证
    count = 0  # k折交叉验证后会出现很多条数据
    # 遍历每一个学生
    for stu in stu_list:
        stu_cour = train_record.loc[[stu], 'course_id'].values
        # print(f' stu {stu} stu_cour{len(stu_cour)}')
        # print(train_record.info)
        stu_activity = train_record.loc[[stu], 'complete'].values
        if len(stu_cour) >= n_splits:
            for train_idx, label_idx in KF.split(stu_cour):
                # 将训练数据划分为训练+标签

                train[0].append(stu)  # 存储学生id
                train[1].append(stu_cour[train_idx])  # 该学生对应的课程id
                train[2].append(stu_activity[train_idx])  # 课程id对应的该学生的分数

                # count是每一行都代表一个数据,代表的是第几条数据，0代表某一行，1代表某一列，
                # 这里的label[2]是真值，取出预测数据的真值
                label[0].extend([count] * len(label_idx))  # 行索引
                label[1].extend(stu_cour[label_idx])  # 列索引
                label[2].extend(stu_activity[label_idx])  # 值
                count += 1
    return train, label


# 测试集
def format_test_data(train_record, test_record):
    train = [[], [], []]  # 学生ID,课程，活跃度
    test = [[], [], []]  # 学生ID,课程，活跃度
    # test_label = [[],[],[]] # 学生，课程，活跃度
    stu_list = set(train_record.index)
    count = 0
    for stu in stu_list:
        stu_cour = train_record.loc[[stu], 'course_id'].values
        stu_activity = train_record.loc[[stu], 'complete'].values
        test_cour = test_record.loc[[stu], 'course_id'].values
        test_activity = test_record.loc[[stu], 'complete'].values

        train[0].append(stu)
        train[1].append(stu_cour)
        train[2].append(stu_activity)

        # 读取测试集的真值
        test[0].extend([count] * len(test_cour))
        test[1].extend(test_cour)
        test[2].extend(test_activity)
        count += 1
    return train, test


# sigmoid是激活函数的一种，它会将样本值映射到0到1之间。
def sigmoid(x):
    return torch.sigmoid(x)


class NewNet(nn.Module):
    # 模型初始化
    # def __init__(self, csa, ca, train_cour_num, behavior_num, device):
    def __init__(self, csa, P, D, train_stu_num,train_cour_num, behavior_num, device):
        super(NewNet, self).__init__()
        # self.P = P
        self.P = P.to(device)
        self.D = D.to(device)
        # self.ca = ca.double().to(device)  # [cid, action]
        self.csa = csa.double()  # [uid, cid, score]
        self.behavior_num = behavior_num

        self.device = device

        self.A = torch.empty(1, self.behavior_num).double().to(self.device)

        # 对每一个课程都有一个猜测率、失误率
        guess = torch.ones(1, train_cour_num).double().to(device)
        slide = torch.ones(1, train_cour_num).double().to(device)

        self.guess_ = nn.Parameter(guess * -2)
        self.slide_ = nn.Parameter(slide * -2)
        # ------------------------------------------

        self.P_A = nn.Parameter(torch.ones_like(self.P, device=self.device))
        self.P_B = nn.Parameter(torch.zeros_like(self.P, device=self.device))
        self.P_C = nn.Parameter(torch.rand(behavior_num, behavior_num, device=self.device).double())
        self.D_A = nn.Parameter(torch.ones_like(self.D, device=self.device))
        self.D_B = nn.Parameter(torch.zeros_like(self.D, device=self.device))
        #self.D_C = nn.Parameter(torch.rand(train_stu_num, train_stu_num, device=self.device).double())

        # self.P_A = nn.Parameter(torch.rand(train_stu_num, 1, device=self.device).double())
        # self.D_A = nn.Parameter(torch.rand(train_stu_num, 1, device=self.device).double())
    def forward(self, stu_list, cour_list, acti_list, save_a=False):  # 前向传播,传入活跃度列表和课程索引列表
        P = torch.mul(self.P, self.P_A) + self.P_B
        D = torch.mul(self.D, self.D_A) + self.D_B

        P = torch.softmax(P, dim=1)
        D = torch.softmax(D, dim=0)



        self.Q = P.T @ (D @ self.P_C)


        A = torch.empty(len(acti_list), self.behavior_num).double().to(self.device)

        # i 学生序号，X_i代表该学生所选课程的活跃度值
        for i, X_i in enumerate(acti_list):
            # 将X_i转成tensor

            X_i = torch.tensor(X_i).double().to(self.device).reshape(1, -1)  # 一行数据 [1, 课程数] 总计有8*折数行的数据
            csa_i = torch.softmax(self.csa[stu_list[i], cour_list[i]].to(self.device),
                                  dim=0)  # 取出一个学生的数据 即1行 [1, 课程数, 19]
            A[i] = X_i @ csa_i  # [1, 课程数] 和 csa_i的转置[19, 课程数] 矩阵相乘 一行为[1, 19] A为 [8*折数, 19]

        if save_a :
            self.A = torch.cat((self.A, A), dim=0)
            print(self.A.shape)


        Y_ = A @ self.Q.T

        # 激活
        slide = sigmoid(self.slide_)
        guess = sigmoid(self.guess_)

        # # 最后的预测值
        Y = (1 - slide) * Y_ + guess * (1 - Y_)

        return Y


    def get_Q(self):
        P = torch.mul(self.P, self.P_A) + self.P_B
        D = torch.mul(self.D, self.D_A) + self.D_B

        P = torch.softmax(P, dim=1)
        D = torch.softmax(D, dim=0)
        self.Q = P.T @ (D @ self.P_C)

        # mask = (self.Q == self.Q.max(dim=1, keepdim=True)[0]).to(dtype=torch.int32)
        # q_sum = torch.sum(mask, dim=0)
        q_sum = torch.sum(self.Q, dim=0)


        torch.save(self.Q, 'Q_weight_MSE.pt')
        print("saved Q")
        print("q sum:")

        print(q_sum)
        return self.Q

    def get_A(self):

        # mask = (self.A == self.A.max(dim=1, keepdim=True)[0]).to(dtype=torch.int32)
        #
        # A_sum = torch.sum(mask, dim=0)
        torch.save(self.A, 'A_weight_MSE.pt')
        print("a saved")

        A_sum = torch.sum(self.A, dim=0)
        print(A_sum)
        return A_sum





class ECPM():
    def __init__(self, csa, P, D,train_stu_num, train_cour_num, behavior_num, lr=1e-3, device='cpu'):

        net = NewNet(csa=csa, P=P, D=D, train_stu_num = train_stu_num,train_cour_num=train_cour_num, behavior_num=behavior_num, device=device)
        self.csa = csa
        # self.ca = ca
        self.device = device
        self.net = net
        # 需要更新的参数有 guess，slide
        self.optimizer = torch.optim.Adam(net.parameters(), lr=lr)

        # self.loss_function = torch.nn.BCELoss(reduction='mean')  # 交叉熵损失函数
        self.loss_function = torch.nn.MSELoss()
        self.all_pred = pd.DataFrame()

    def fit(self, index_loader, train_data, test_data,
            epochs,
            save_dir=None, save_size=None):


        for epoch in range(epochs):

            loss_list = [[]]  # [[train]]
            label_list, pred_list = [[]], [[]]  # [[train]]
            for batch_data in tqdm(index_loader, "[Epoch:%s]" % epoch):

                stu_list = np.array([x.numpy() for x in batch_data], dtype='int').reshape(-1)

                train_pred, label_data = format_data(train_data.loc[stu_list, :])


                all_pred = self.net(train_pred[0], train_pred[1], train_pred[2], save_a=False)

                pred = all_pred[label_data[0], label_data[1]]
                # print(f'pred {pred}')

                label = torch.DoubleTensor(label_data[2]).to(self.device)
                # print(f'label {label}')
                loss = self.loss_function(pred, label)
                loss_list[0].append(loss.item())
                pred_list[0].extend(pred.clone().to('cpu').detach().tolist())

                label_list[0].extend(label_data[2])


                # ------start update parameters----------
                self.optimizer.zero_grad()
                loss.backward()
                self.optimizer.step()

                # ------ end update parameters-----------

            # -------start evaluate and drawing-----------
            # print(f'loss {loss_list}')
            epoch_loss = np.nanmean(loss_list[0])
            rmse, mae = evaluate(pred_list[0], label_list[0])
            print(f'epoch_loss {epoch_loss}  rmse {rmse} mae {mae}')

            # 画出训练的loss以及rmse和mae曲线
            # viz.line([[epoch_loss]], [epoch], win='train loss', update='append')
            # viz.line([[rmse, mae]], [epoch], win='test rmse&mae', update='append')

            if save_size is not None:
                test_rmse, test_mae, test_loss = self.test(index_loader, train_data, test_data, save_dir=save_dir,
                                                           save_size=save_size, is_final_epoch = epoch is epochs-1) # 最后一个epoch保存
                viz.line([[epoch_loss, test_loss]], [epoch], win='xuetangx loss', update='append')
                viz.line([[rmse, test_rmse, mae, test_mae]], [epoch], win='xuetangx rmse&mae', update='append')

                # self.test(index_loader,train_data,valid_data, save_dir=save_dir, save_size=save_size)
            # self.all_pred_data(index_loader, train_data, valid_data)
            # -------end evaluate and drawing-----------

    def test(self, index_loader, train_data, test_data, save_dir=None, save_size=None, is_final_epoch=False):
        test_loss_list = [[]]
        test_pred_list, test_label_list = [], []
        if save_size is not None:
            all_pred_score = torch.empty(save_size, dtype=torch.double).to('cpu')
        for batch_data in tqdm(index_loader, "[Testing:]"):
            stu_list = np.array([x.numpy() for x in batch_data], dtype='int').reshape(-1)
            # train_data.loc[stu_list, :]
            # test_data.loc[stu_list, :]
            train, test = format_test_data(train_data.loc[stu_list, :],
                                           test_data.loc[stu_list, :])

            with torch.no_grad():

                all_pred = self.net(train[0], train[1], train[2], save_a=is_final_epoch)
                # print(all_pred.size())

                test_pred = all_pred[test[0], test[1]].clone().to('cpu').detach()
                test_label = torch.DoubleTensor(test[2])
                test_loss = self.loss_function(test_pred, test_label)

                test_pred_list.extend(test_pred.tolist())

                test_label_list.extend(test[2])

                test_loss_list[0].append(test_loss.item())
                epoch_test_loss = np.mean(test_loss_list[0])

                # -------record -----------
                if save_size is not None:
                    all_pred_score[torch.LongTensor(train[0])] = all_pred.cpu().detach()
        rmse, mae = evaluate(test_pred_list, test_label_list)
        print("\t test_result: loss %.6f rmse:%.6f, mae:%.6f" % (epoch_test_loss,rmse, mae))
        if save_size is not None:
            np.savetxt(save_dir + 'pred_score.csv', all_pred_score.numpy(),
                       fmt='%.6f', delimiter=',')
        return rmse, mae, epoch_test_loss

    def save_parameter(self, save_dir):
        torch.save(self.net.state_dict(), './model_save/NewNet_weight.pth')
        print("model saved")
        # np.savetxt(save_dir + 'slide_.txt', self.net.slide_.cpu().detach().numpy())
        # np.savetxt(save_dir + 'guess_.txt', self.net.guess_.cpu().detach().numpy())
        # self.all_pred.to_csv(save_dir + 'pred_data.csv', sep=',')


    def load_net(self, path):
        checkpoint = torch.load(path)
        self.net.load_state_dict(checkpoint)