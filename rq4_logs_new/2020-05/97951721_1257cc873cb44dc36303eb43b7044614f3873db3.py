import torch 
import torch.nn as nn
import torch.nn.functional as F

criterion = nn.KLDivLoss(reduction='batchmean')

x = torch.tensor([[0.0, 1.0]])
x = F.log_softmax(x, dim=1)
y = torch.tensor([[0.05, 0.95]])


print(F.kl_div(x, y, reduction='mean'))

print(criterion(x, y))


class LabelSmoothing(nn.Module):

    "Implement label smoothing."

    def __init__(self, size, padding_idx, smoothing=0.0):

        super(LabelSmoothing, self).__init__()

        self.criterion = nn.KLDivLoss(reduction='batchmean')

        self.padding_idx = padding_idx

        self.confidence = 1.0 - smoothing

        self.smoothing = smoothing

        self.size = size

        self.true_dist = None



    def forward(self, x, target):

        assert x.size(1) == self.size

        true_dist = x.data.clone()

        true_dist.fill_(self.smoothing / (self.size - 1))

        true_dist.scatter_(1, target.data.unsqueeze(1), self.confidence)

        # true_dist[:, self.padding_idx] = 0

        # mask = torch.nonzero(target.data == self.padding_idx)

        # if mask.dim() > 0:

        #     true_dist.index_fill_(0, mask.squeeze(), 0.0)

        self.true_dist = true_dist

        return self.criterion(x, true_dist.requires_grad_(False))


class LabelSmoothingLoss(nn.Module):
    """
    With label smoothing,
    KL-divergence between q_{smoothed ground truth prob.}(w)
    and p_{prob. computed by model}(w) is minimized.
    """
    def __init__(self, label_smoothing, tgt_vocab_size, ignore_index=-100):
        assert 0.0 < label_smoothing <= 1.0
        self.ignore_index = ignore_index
        super(LabelSmoothingLoss, self).__init__()

        smoothing_value = label_smoothing / (tgt_vocab_size - 1)
        one_hot = torch.full((tgt_vocab_size,), smoothing_value)
       # one_hot[self.ignore_index] = 0
        self.register_buffer('one_hot', one_hot.unsqueeze(0))

        self.confidence = 1.0 - label_smoothing

    def forward(self, output, target):
        """
        output (FloatTensor): batch_size x n_classes
        target (LongTensor): batch_size
        """
        model_prob = self.one_hot.repeat(target.size(0), 1)
        model_prob.scatter_(1, target.unsqueeze(1), self.confidence).to(0)
      #  model_prob.masked_fill_((target == self.ignore_index).unsqueeze(1), 0)

        return F.kl_div(output, model_prob, reduction='sum')

# if __name__ == "__main__":
#     crit = LabelSmoothing(size=5, padding_idx=0, smoothing=0.1)
#     crit1 = LabelSmoothingLoss(0.1, 5, None)
#     # predict.shape 3 5
#     predict = torch.FloatTensor([[0, 0.7, 0.1, 0, 0.1],
#                                  [0, 0.9, 0.2, 0.1, 0],
#                                  [1, 0.2, 0.7, 0.1, 0]])
    
#    # v = crit(Variable(predict.log()), torch.tensor([1, 1, 0]))
#    # preprecess = torch.nn.LogSoftmax(dim=1)
#     v = crit(F.log_softmax(predict, dim=1), torch.tensor([1, 1, 0]))
#     v1 = crit1(F.log_softmax(predict), torch.tensor([1, 1, 0]))
#     print(v)
#     print(v1)