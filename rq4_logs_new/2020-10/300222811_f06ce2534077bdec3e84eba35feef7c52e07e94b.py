import torch
import torch.nn as nn

# ref: https://amaarora.github.io/2020/06/29/FocalLoss.html
class FocalLoss(nn.Module):

    def __init__(self, device, alpha=.25, gamma=2):
        super(FocalLoss, self).__init__()
        self.alpha = torch.tensor([alpha, 1 - alpha]).to(device)
        self.gamma = gamma

        self.ce = nn.CrossEntropyLoss(reduction="none")

    def forward(self, inputs, targets):
        CE_loss = self.ce(inputs, targets)
        targets = targets.type(torch.long)
        at = self.alpha.gather(0, targets.data.view(-1))
        pt = torch.exp(-CE_loss)
        F_loss = at * (1 - pt) ** self.gamma * CE_loss
        return F_loss.mean()