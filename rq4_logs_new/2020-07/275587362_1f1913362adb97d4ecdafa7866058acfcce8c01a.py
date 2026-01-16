import torch
import math

conv_channels = 32

def get_default_device():
  return torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

def is_learning_module(module: torch.nn.Module): return hasattr(module, 'weight')

def make(classes_len, img_size, input_channels = 3):
  block = {
    "conv1": torch.nn.Conv2d(input_channels, conv_channels, 3, padding=1),
    "bnorm1": torch.nn.BatchNorm2d(conv_channels),
    "relu1": torch.nn.ReLU(),
    # "conv2": torch.nn.Conv2d(conv_channels, conv_channels, 3, padding=1),
    # "bnorm2": torch.nn.BatchNorm2d(conv_channels),
    "conv_out_merge": lambda activation: activation.view(-1, conv_channels * img_size[0] * img_size[1]),
    "linear1": torch.nn.Linear(conv_channels * img_size[0] * img_size[1], 50),
    "relu2": torch.nn.ReLU(),
    "linear2": torch.nn.Linear(50, classes_len),
    "norm1": torch.nn.BatchNorm1d(classes_len),
  }
  block = init(block)
  return block

def init(block, device = None):
  device = device if device is not None else get_default_device()
  for item_key in list(block):
    block_item = block[item_key]
    if hasattr(block_item, 'to'): block_item.to(device)
    if is_learning_module(block_item):
      block_item.momentum_val = torch.empty_like(block_item.weight).fill_(0.)
      block_item.momentum_sqr_val = torch.empty_like(block_item.weight).fill_(0.)
  return block

def predict(block, data):
  input_pipe = data
  for item_key in list(block):
    block_item = block[item_key]
    # print("predict", block_item)
    input_pipe = block_item(input_pipe)
  return input_pipe

def calc_weights(block):
  wd_sqr = 0
  for item_key in list(block):
    block_item = block[item_key]
    if is_learning_module(block_item):
      wd_sqr += block_item.weight.data.pow(2).sum()
  return wd_sqr

def back_propagate(block, lr, momentum, wd_part = 1):
  for item_key in list(block):
    block_item = block[item_key]
    if not is_learning_module(block_item): continue
    model: torch.nn.Module = block_item
    grad = model.weight.grad.data
    momentum_val = (model.momentum_val * momentum) + (grad * (1 - momentum)) + 0.0001
    momentum_sqr_val = (model.momentum_sqr_val * momentum) + ((grad.pow(2)) * (1 - momentum)) + 0.0001
    grad_dims = len(grad.size())
    grad_pick = tuple(torch.zeros(grad_dims, dtype=int).tolist())
    if math.isnan(grad[grad_pick]):
      print("model", model)
      print("grad", grad)
      print("momentum_val", momentum_val)
    update = (momentum_val * lr)# / momentum_sqr_val.sqrt()) * wd_part

    model.momentum_val = momentum_val
    model.momentum_sqr_val = momentum_sqr_val
    model.weight.data.sub_(update)
    model.zero_grad()