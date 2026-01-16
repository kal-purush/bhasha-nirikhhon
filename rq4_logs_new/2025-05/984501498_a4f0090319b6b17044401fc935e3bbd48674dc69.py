import os
import sys
import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.autograd as autograd



# the feature extractor
class DeepConvNet(nn.Module):
    
    def __init__(self, 
                 n_chan = 22,
                 n_time = 500,
                 n_class = 2,
                 n_filter_temp=25,
                 n_filter_spat=25,
                 pool_time_length=2,
                 pool_time_stride=2,
                 n_filter_2=50,
                 filter_length_2=10,
                 n_filter_3=100,
                 filter_length_3=10,
                 n_filter_4=200,
                 filter_length_4=10,
                 dropout=0.5,
                 ):
        super().__init__()

        self.block1 = nn.Sequential(
            nn.Conv2d(1, n_filter_temp, (1,5), stride=1),
            nn.Conv2d(n_filter_temp, n_filter_spat, (n_chan,1), stride=1),
            nn.BatchNorm2d(n_filter_spat),
            nn.ELU(),
            nn.MaxPool2d(kernel_size=(1,pool_time_length), stride=(1,pool_time_stride))
        )

        self.block2 = nn.Sequential(
            nn.Dropout(dropout),
            nn.Conv2d(n_filter_spat, n_filter_2, (1,filter_length_2), stride=1),
            nn.BatchNorm2d(n_filter_2),
            nn.ELU(),
            nn.MaxPool2d(kernel_size=(1,pool_time_length), stride=(1,pool_time_stride))
        )

        self.block3 = nn.Sequential(
            nn.Dropout(dropout),
            nn.Conv2d(n_filter_2, n_filter_3, (1,filter_length_3), stride=1),
            nn.BatchNorm2d(n_filter_3),
            nn.ELU(),
            nn.MaxPool2d(kernel_size=(1,pool_time_length), stride=(1,pool_time_stride))
        )

        self.block4 = nn.Sequential(
            nn.Dropout(dropout),
            nn.Conv2d(n_filter_3, n_filter_4, (1,filter_length_4), stride=1),
            nn.BatchNorm2d(n_filter_4),
            nn.ELU(),
            nn.MaxPool2d(kernel_size=(1,pool_time_length), stride=(1,pool_time_stride))
        )


    def forward(self, input):
        
        if len(input.shape) == 3:
            x = torch.unsqueeze(input, 1)
        elif len(input.shape) == 4:
            x = input.clone()

        x = torch.transpose(x, 2, 3)

        # x (b, 1, n_chan, n_time)

        output = self.block1(x)
        output = self.block2(output)
        output = self.block3(output)
        output = self.block4(output)

        # output = torch.squeeze(output)

        return output



class Disentangler(nn.Module):

    def __init__(self, args):
        super().__init__()

        self.fc = nn.Linear(in_features=args.emb_dim, out_features=args.emb_dim)
        self.bn = nn.BatchNorm1d(args.emb_dim)

    def forward(self, x):
        output = self.fc(x)
        output = self.bn(output)
        
        return output



class Decoder(nn.Module):

    def __init__(self, args):
        super().__init__()

        self.fc = nn.Linear(in_features=args.emb_dim, out_features=args.emb_dim)
        self.bn = nn.BatchNorm1d(args.emb_dim)

    def forward(self, x):
        output = self.fc(x)
        output = self.bn(output)

        return output


class CZSL(nn.Module):

    def __init__(self, args):
        super(CZSL, self).__init__()

        self.device = args.device

        self.num_subj = args.num_subj
        self.num_task = args.num_task
        self.lambda_rep = args.lambda_rep
        self.lambda_rec = args.lambda_rec
        self.lambda_swap = args.lambda_swap
        self.lambda_grd = args.lambda_grd
        self.start_epoch = args.start_epoch
        self.drop = args.drop

        self.feat_extractor = DeepConvNet(n_chan=args.n_chan)
        feat_dim = 200

        self.emb_dim = args.emb_dim

        self.encoder = nn.Sequential(
            nn.Conv2d(in_channels=feat_dim, out_channels=args.emb_dim, kernel_size=1, bias=False),
            nn.BatchNorm2d(args.emb_dim),
            nn.ReLU()
        )
        self.avg_pool = nn.AdaptiveAvgPool2d((1,1))

        self.disen_subj = Disentangler(args)
        self.disen_task = Disentangler(args)

        self.clf_subj = nn.Linear(in_features=args.emb_dim, out_features=args.num_subj)
        self.clf_task = nn.Linear(in_features=args.emb_dim, out_features=args.num_task)

        self.decoder = Decoder(args)


    def forward(self, x, epoch):

        if self.training:
            loss, scores = self.train_forward(x, epoch)
        else:
            with torch.no_grad():
                loss, scores = self.test_forward(x)
        
        return loss, scores


    def train_forward(self, x, epoch):

        sample, same_subj_sample, same_task_sample, subj, task, diff_task, diff_subj = x

        sample = sample.to(self.device)
        same_subj_sample = same_subj_sample.to(self.device)
        same_task_sample = same_task_sample.to(self.device)
        subj = subj.to(self.device)
        task = task.to(self.device)
        diff_task = diff_task.to(self.device)
        diff_subj = diff_subj.to(self.device)

        # Extract feature
        sample = self.feat_extractor(sample)
        sample = self.encoder(sample)
        sample_feat = self.avg_pool(sample).squeeze()

        same_subj_sample = self.feat_extractor(same_subj_sample)
        same_subj_sample = self.encoder(same_subj_sample)
        same_subj_feat = self.avg_pool(same_subj_sample).squeeze()

        same_task_sample = self.feat_extractor(same_task_sample)
        same_task_sample = self.encoder(same_task_sample) 
        same_task_feat = self.avg_pool(same_task_sample).squeeze()


        # Disentangle
        sample_ds = self.disen_subj(sample_feat)
        sample_dt = self.disen_task(sample_feat)

        same_subj_ds = self.disen_subj(same_subj_feat)
        diff_task_dt = self.disen_task(same_subj_feat)

        diff_subj_ds = self.disen_subj(same_task_feat)
        same_task_dt = self.disen_task(same_task_feat)


        # Classify
        sample_ds_pred = self.clf_subj(sample_ds)
        # sample_dt_pred = self.clf_task(sample_dt)
        sample_dt_pred = self.clf_task(sample_dt+sample_dt)
        loss_sample_subj = F.cross_entropy(sample_ds_pred, subj)
        loss_sample_task = F.cross_entropy(sample_dt_pred, task)

        same_subj_ds_pred = self.clf_subj(same_subj_ds)
        diff_task_dt_pred = self.clf_task(diff_task_dt)
        loss_same_subj = F.cross_entropy(same_subj_ds_pred, subj)
        loss_diff_task = F.cross_entropy(diff_task_dt_pred, diff_task)

        diff_subj_ds_pred = self.clf_subj(diff_subj_ds)
        same_task_dt_pred = self.clf_task(same_task_dt)
        loss_diff_subj = F.cross_entropy(diff_subj_ds_pred, diff_subj)
        loss_same_task = F.cross_entropy(same_task_dt_pred, task)

        loss_clf = loss_sample_subj + loss_sample_task + loss_same_subj + loss_diff_task + loss_diff_subj + loss_same_task

        # invarient representation learning mechanism
        subj_onehot = F.one_hot(subj, self.num_subj)
        task_onehot = F.one_hot(task, self.num_task)

        sample_ds_grad = autograd.grad((sample_ds_pred * subj_onehot).sum(), sample_feat, retain_graph=True)[0]
        same_subj_grad = autograd.grad((same_subj_ds_pred * subj_onehot).sum(), same_subj_feat, retain_graph=True)[0]

        sample_dt_grad = autograd.grad((sample_dt_pred * task_onehot).sum(), sample_feat, retain_graph=True)[0]
        same_task_grad = autograd.grad((same_task_dt_pred * task_onehot).sum(), same_task_feat, retain_graph=True)[0]

        grad_simi_subj = torch.abs(sample_ds_grad - same_subj_grad)
        # perct_subj = torch.sort(grad_simi_subj)[0][:, int(self.drop*self.emb_dim)]
        perct_subj = torch.sort(grad_simi_subj)[0][:, int(self.drop*self.emb_dim)]
        perct_subj = perct_subj.unsqueeze(1).repeat(1, self.emb_dim)
        mask_subj = grad_simi_subj.lt((perct_subj)).float()

        grad_simi_task = torch.abs(sample_dt_grad - same_task_grad)
        # perct_task = torch.sort(grad_simi_task)[0][:,int(self.drop * self.emb_dim)]
        perct_task = torch.sort(grad_simi_task)[0][:, int(self.drop*self.emb_dim)]
        perct_task = perct_task.unsqueeze(1).repeat(1, self.emb_dim)
        mask_task = grad_simi_task.lt(perct_task).float()

        loss_rep_subj1 = F.cross_entropy(self.clf_subj(self.disen_subj(sample_feat * mask_subj)), subj)
        loss_rep_task1 = F.cross_entropy(self.clf_task(self.disen_task(sample_feat * mask_task)), task)

        loss_rep_subj2 = F.cross_entropy(self.clf_subj(self.disen_subj(same_subj_feat * mask_subj)), subj)
        loss_rep_task2 = F.cross_entropy(self.clf_task(self.disen_task(same_task_feat * mask_task)), task)

        loss_rep = loss_rep_subj1 + loss_rep_subj2 + loss_rep_task1 + loss_rep_task2

        # reconstruction
        recon_sample = self.decoder(torch.concat((sample_ds, sample_dt), dim=1))
        recon_same_subj = self.decoder(torch.concat((same_subj_ds, diff_task_dt), dim=1))
        recon_same_task = self.decoder(torch.concat((diff_subj_ds, same_task_dt), dim=1))

        loss_rec = F.mse_loss(sample_feat.detach(), recon_sample) + F.mse_loss(same_subj_feat.detach(), recon_same_subj) + F.mse_loss(same_task_feat.detach(), recon_same_task)

        loss = loss_clf + self.lambda_rep * loss_rep + self.lambda_rec * loss_rec

        # indeterminacy decoupling
        if epoch >= self.start_epoch:

            subj_feat = [sample_ds, same_subj_ds, diff_subj_ds]
            task_feat = [sample_dt, diff_task_dt, same_task_dt]

            subj_label = [subj, subj, diff_subj]
            task_label = [task, diff_task, task]

            subj_reshuffle_index = torch.randperm(3)
            task_reshuffle_index = torch.randperm(3)

            new_subj_feat = [0,0,0]
            new_task_feat = [0,0,0]
            new_comp = [0,0,0]

            loss_swap_subj = 0.0
            loss_swap_task = 0.0

            for i in range(3):

                new_subj_feat[i] = subj_feat[subj_reshuffle_index[i]]
                new_task_feat[i] = task_feat[task_reshuffle_index[i]]

                new_comp[i] = self.decoder(torch.cat((new_subj_feat[i], new_task_feat[i]), dim=1))

                loss_swap_subj += F.cross_entropy(self.clf_subj(self.disen_subj(new_comp[i])), subj_label[subj_reshuffle_index[i]])
                loss_swap_task += F.cross_entropy(self.clf_task(self.disen_task(new_comp[i])), task_label[task_reshuffle_index[i]])

            loss_swap = loss_swap_subj + loss_swap_task
            loss += self.lambda_swap * loss_swap


        return loss, None
    

    def test_forward(self, x):
        
        sample = x[0].to(self.device)
        subj = x[1].to(self.device)
        task = x[2].to(self.device)

        sample = self.feat_extractor(sample)
        sample_sample = self.encoder(sample)
        sample_feat = self.avg_pool(sample_sample).squeeze()

        sample_ds = self.disen_subj(sample_feat)
        sample_dt = self.disen_task(sample_feat)

        sample_ds_pred = self.clf_subj(sample_ds)
        # sample_dt_pred = self.clf_task(sample_dt)
        sample_dt_pred = self.clf_task(sample_dt + sample_ds)

        # accuracy
        sample_ds_pred = sample_ds_pred.argmax(dim=1)
        sample_dt_pred = sample_dt_pred.argmax(dim=1)


        return None, [sample_ds_pred, sample_dt_pred]


