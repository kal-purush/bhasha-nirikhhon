import torch
import datetime
from network.srtg_resnet import srtg_r2plus1d_50
from data import iterator_factory
from train import metric
from train.model import model

#%% Define params
# define path
dataset_path = '/nfs/s2/userhome/zhouming/workingdir/Video/HACS/dataset'
working_path = '/nfs/s2/userhome/zhouming/workingdir/Video/HACS/train_model'
output_path  = f'{working_path}/out'

#%% prepare model
net = srtg_r2plus1d_50(num_classes=200)
net = torch.nn.DataParallel(net, device_ids=[0])
model = model(net=net,
              criterion=torch.nn.CrossEntropyLoss().cuda())
info = torch.load(f'{working_path}/models/srtg_r2plus1d_50_best.pth')
model.load_state(info['state_dict'],strict=True)
del info

#%%  prepare dataset
# data iterator - randomisation based on date and time values
iter_seed = torch.initial_seed() + 100 
now = datetime.datetime.now()
iter_seed += now.year + now.month + now.day + now.hour + now.minute + now.second

# Create custom loaders for validation
eval_loader = iterator_factory.create(
    name='HACS',
    batch_size=8,
    val_clip_length=16,
    val_clip_size=264,
    val_interval=2,
    seed=iter_seed,
    data_root=dataset_path)

# define evaluation metric
metrics = metric.MetricList(metric.Loss(name="loss-ce"),
                            metric.Accuracy(name="top1", topk=1),
                            metric.Accuracy(name="top5", topk=5),
                            metric.BatchSize(name="batch_size"),
                            metric.LearningRate(name="lr"))


# Evaluation happens here
val_top1_sum = []
val_top5_sum = []
val_loss_sum = []

with torch.no_grad():
    model.net.eval()
    metrics.reset()
    
    for i_batch, (data, target, path) in enumerate(eval_loader):
        # [forward] making next step
        torch.cuda.empty_cache()
        outputs, losses = model.forward(data, target)
        metrics.update([output.data.cpu() for output in outputs],
                        target.cpu(),
                        [loss.data.cpu() for loss in losses])
        
        m = metrics.get_name_value()
        val_top1_sum.append(m[1][0][1])
        val_top5_sum.append(m[2][0][1])
        val_loss_sum.append(m[0][0][1])
        
        if (i_batch%50 == 0):
            val_top1_avg = sum(val_top1_sum)/(i_batch+1)
            val_top5_avg = sum(val_top5_sum)/(i_batch+1)
            val_loss_avg = sum(val_loss_sum)/(i_batch+1)
            print('Iteration [{:d}]:  (val)  average top-1 acc: {:.5f}  ' \
                  'average top-5 acc: {:.5f}   average loss {:.5f}'.format \
                  (i_batch,val_top1_avg,val_top5_avg,val_loss_avg))
        # store input names and output
        video_names = [x.split('/')[-1] for x in list(path)]

        
l = len(val_top1_sum)
val_top1_dataset = sum(val_top1_sum)/l
val_top5_dataset = sum(val_top5_sum)/l
val_loss_dataset = sum(val_loss_sum)/l
print('Total Validation Set: average top-1 acc: {:.5f}  ' \
      'average top-5 acc: {:.5f}   average loss {:.5f}'.format \
      (val_top1_dataset,val_top5_dataset,val_loss_dataset))





