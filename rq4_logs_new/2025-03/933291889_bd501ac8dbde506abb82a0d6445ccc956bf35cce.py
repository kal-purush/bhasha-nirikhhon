import torch

xs = torch.Tensor([
    (1, 2),
    (3, 4)
]).long()

x1 = torch.nn.functional.one_hot(xs[:, 0], num_classes=27)
x2 = torch.nn.functional.one_hot(xs[:, 1], num_classes=27)
xenc = torch.cat((x1, x2), dim=1)

print(xenc)