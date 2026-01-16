from graphdiffusion.pipeline import *

import pytest 
from torch import nn 

def dummy_callable(x, x1=1, x2=2, x3=3, x4=4): 
    return x

def dummy_callable_inference(x, x1=1, x2=2, x3=3, x4=4): 
    if x2 is not None and isinstance(x2, torch.Tensor):
        return x2, x2
    return x, x

class DummyModule(nn.Module):
    def __init__(self):
        super(DummyModule, self).__init__()
    def forward(self, x, x1=1, x2=2, x3=3, x4=4):
        return x

def test_create_base():
    

    node_feature_dim = 2
    device = torch.device("cpu")
    reconstruction_obj = DummyModule()
    inference_obj = dummy_callable
    degradation_obj = dummy_callable
    train_obj = dummy_callable
    bridge_obj = dummy_callable
    distance_obj = dummy_callable
    encoding_obj = dummy_callable
    trainable_objects = None
    pre_trained_path = None
    
    pipeline = PipelineBase(node_feature_dim, device, reconstruction_obj, inference_obj, degradation_obj, train_obj, bridge_obj, distance_obj, encoding_obj, trainable_objects, pre_trained_path)
    return pipeline


def test_train_base():
    pipeline = test_create_base()
    example_tensor = torch.randn(5) * 10 
    pipeline.train(example_tensor)

def test_getmodel_base():
    pipeline = test_create_base()
    pipeline.get_model()
    with pytest.raises(AssertionError):
        pipeline.define_trainable_objects(True, True, True, True)

def test_getmodel_base():
    pipeline = test_create_base() 
    pipeline.get_model()
    pipeline.degradation_obj = DummyModule()
    pipeline.distance_obj = DummyModule()
    pipeline.encoding_obj = DummyModule()
    pipeline.define_trainable_objects(True, True, True, True)
    pipeline.get_model()


def test_default_methods_base():
    pipeline = test_create_base()
    x = pipeline.distance(1,1)
    x = pipeline.bridge(1,2,3,4)
    x = pipeline.reconstruction(1,2)
    x = pipeline.degradation(1,2)
    with pytest.raises(ValueError):
        x = pipeline.inference(1,2)
    pipeline.inference_obj = dummy_callable_inference
    x = pipeline.inference(1,2)


from graphdiffusion.plotting import plot_data_as_normal_pdf

def test_forward_base():
    pipeline = test_create_base()
    input_tensor = torch.rand(5,5)
    pipeline.visualize_foward(input_tensor, outfile=None, num=9, plot_data_func=plot_data_as_normal_pdf)

def test_forward_base2():
    pipeline = test_create_base()
    input_tensor = torch.rand(5,5)
    pipeline.visualize_foward(input_tensor, outfile=None, num=9, plot_data_func=plot_array_on_axis)


#visualize_reconstruction(self, data, outfile, outfile_projection, num, steps, plot_data_func)

def test_recon_base():
    pipeline = test_create_base()
    input_tensor = torch.rand(5,5)
    with pytest.raises(ValueError):
        pipeline.visualize_reconstruction(input_tensor, outfile='outfile_test.jpg', outfile_projection='outfile_test_proj.jpg', num=90, steps=9, plot_data_func=plot_array_on_axis)
    with pytest.raises(ValueError):
        pipeline.visualize_reconstruction(input_tensor, outfile='outfile_test.jpg', outfile_projection='outfile_test_proj.jpg', num=9, steps=90, plot_data_func=plot_array_on_axis)
    pipeline.inference_obj = dummy_callable_inference

    pipeline.visualize_reconstruction(data=input_tensor, outfile='outfile_test_809283904.jpg', outfile_projection='outfile_test_proj_809283904.jpg', num=9, steps=90, plot_data_func=plot_array_on_axis)
    os.system("rm outfile_test_809283904.jpg")
    os.system("rm outfile_test_proj_809283904.jpg")


def test_compare_base():
    from torch_geometric.loader import DataLoader

    def dummy_callable_distance(x, x1=1, x2=2, x3=3, x4=4): 
        return torch.tensor(1.0)

    pipeline = test_create_base()
    pipeline.distance_obj = dummy_callable_distance

    # compare_distribution(self, real_data, generated_data, batch_size, num_comparisions, outfile, max_plot, compare_data_batches_func, **kwargs)
    real_data = DataLoader([torch.rand(5) for _ in range(10)], batch_size=5, shuffle=True)
    generated_data = DataLoader([torch.rand(5) for _ in range(10)], batch_size=5, shuffle=True)

    batch_size = 5
    num_comparisions = 10
    outfile = "outfile_test_dist_809283904.jpg"
    max_plot = 5
    compare_data_batches_func = graphdiffusion.compare_distributions.compare_data_batches
    pipeline.compare_distribution(real_data, generated_data, batch_size, num_comparisions, outfile, max_plot, compare_data_batches_func)

    os.system("rm outfile_test_dist_809283904.jpg")

