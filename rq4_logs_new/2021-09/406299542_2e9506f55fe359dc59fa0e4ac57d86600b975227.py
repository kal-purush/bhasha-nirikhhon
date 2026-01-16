import torch
import torchvision
import onnx

from onnx import checker, shape_inference
from pathlib import Path

def infer_shape(fpath):
    model_onnx = onnx.load(fpath)
    checker.check_model(model_onnx)

    model_infered = shape_inference.infer_shapes(model_onnx)
    checker.check_model(model_infered)
    fpath = Path(fpath)
    saved_path = fpath.parent.joinpath(fpath.stem + "_infered" + fpath.suffix)
    onnx.save(model_infered, saved_path)

if __name__ == "__main__":
    opset = 11

    dummy_input = torch.randn(4, 3, 224, 224)
    model = torchvision.models.resnet101(pretrained=True).eval()
    # model = torch.nn.Sequential(
    #     model,
    #     torch.nn.Softmax(dim=1)
    # ).eval()
    model.requires_grad_(False)

    input_name = ["input"]
    output_name = ["output"]

    ## fixed shape input
    print("Exporting resnet101 with fixed batch size")
    torch.onnx.export(model, dummy_input, "data/resnet101_pt.onnx", verbose=True,
        opset_version=opset, input_names=input_name, output_names=output_name)

    ## dynamic batch
    print("Exporting resnet101 with dynamic batch size")
    bs_name = "batch_size"
    dynamic_axes = {input_name[0]: {0: bs_name}, output_name[0]: {0: bs_name}}
    torch.onnx.export(model, dummy_input, "data/resnet101_dynamic.onnx", verbose=True,
        opset_version=opset, input_names=input_name, output_names=output_name,
        dynamic_axes=dynamic_axes)

    infer_shape("data/resnet101_pt.onnx")
    infer_shape("data/resnet101_dynamic.onnx")