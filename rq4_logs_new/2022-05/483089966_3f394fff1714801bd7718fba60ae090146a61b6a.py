""" Define config settings for all the different experiements to run. """
from detectron2.config import CfgNode as CN


def coco_dataset_config(cfg):
    raise NotImplementedError


def voc_dataset_config(cfg):
    cfg.voc = CN()
    cfg.voc.root = "../data/VOC/"
    cfg.voc.year = "2012"
    cfg.voc.image_set = "train"
    cfg.voc.transforms = "voc_transforms"
    cfg.voc.corruption_transforms = "voc_corruption_transforms"
    cfg.MODEL.ROI_HEADS.NUM_CLASSES = 20


def sift_resnet_config(cfg, args):
    if args.dataset == "COCO":
        coco_dataset_config(cfg)
    else:
        voc_dataset_config(cfg)

    cfg.MODEL.BACKBONE.NAME = "build_resnet50_backbone"
    cfg.MODEL.RESNETS.OUT_FEATURES = ["layer4"]
    cfg.MODEL.FPN.IN_FEATURES = ["layer4"]
    cfg.MODEL.ANCHOR_GENERATOR.SIZES = [[32], [64], [128], [256], [512]]
    cfg.MODEL.ANCHOR_GENERATOR.ASPECT_RATIOS = [[0.5, 1.0, 2.0]]
    cfg.MODEL.RPN.IN_FEATURES = [["layer4"]]
    cfg.MODEL.ROI_HEADS.NAME = "StandardROIHeads"
    cfg.MODEL.ROI_HEADS.IN_FEATURES = ["p4"]
    cfg.MODEL.ROI_BOX_HEAD.NAME = "FastRCNNConvFCHead"
    cfg.MODEL.ROI_BOX_HEAD.NUM_FC = 2
    cfg.MODEL.ROI_BOX_HEAD.POOLER_RESOLUTION = 14


def sift_convnext_config(cfg, args):
    if args.dataset == "COCO":
        coco_dataset_config(cfg)
    else:
        voc_dataset_config(cfg)

    cfg.MODEL.BACKBONE.NAME = "build_convnext_backbone"


def bar_resnet_config(cfg, args):
    if args.dataset == "COCO":
        coco_dataset_config(cfg)
    else:
        voc_dataset_config(cfg)

    cfg.MODEL.BACKBONE.NAME = "build_resnet50_backbone"


def bar_convnext_config(cfg, args):
    if args.dataset == "COCO":
        coco_dataset_config(cfg)
    else:
        voc_dataset_config(cfg)

    cfg.MODEL.BACKBONE.NAME = "build_convnext_backbone"


def faster_rcnn_resnet_config(cfg, args):
    if args.dataset == "COCO":
        coco_dataset_config(cfg)
    else:
        voc_dataset_config(cfg)

    # Model Architecture
    cfg.MODEL.META_ARCHITECTURE = "GeneralizedRCNN"

    # Backbone
    cfg.MODEL.BACKBONE.NAME = "build_resnet50_fpn_backbone"
    cfg.MODEL.RESNETS.OUT_FEATURES = ["layer1", "layer2", "layer3", "layer4"]

    # FPN
    cfg.MODEL.FPN.IN_FEATURES = ["layer1", "layer2", "layer3", "layer4"]

    # Anchor Generator
    cfg.MODEL.ANCHOR_GENERATOR.SIZES = [[32, 64, 128, 256, 512]]
    cfg.MODEL.ANCHOR_GENERATOR.ASPECT_RATIOS = [[0.5, 1.0, 2.0]]

    # RPN
    cfg.MODEL.RPN.IN_FEATURES = ["p2", "p3", "p4", "p5"]

    # ROI
    cfg.MODEL.ROI_HEADS.NAME = "StandardROIHeads"
    cfg.MODEL.ROI_HEADS.IN_FEATURES = ["p2", "p3", "p4", "p5"]
    cfg.MODEL.ROI_BOX_HEAD.NAME = "FastRCNNConvFCHead"
    cfg.MODEL.ROI_BOX_HEAD.NUM_FC = 2
    cfg.MODEL.ROI_BOX_HEAD.POOLER_RESOLUTION = 14

    # Optimizer configs
    cfg.SOLVER.IMS_PER_BATCH = 16
    cfg.SOLVER.BASE_LR = 0.02
    cfg.SOLVER.STEPS = (60000, 80000)
    cfg.SOLVER.MAX_ITER = 90000


def faster_rcnn_convnext_config(cfg, args):
    if args.dataset == "COCO":
        coco_dataset_config(cfg)
    else:
        voc_dataset_config(cfg)

    # Model architecture configs
    cfg.MODEL.META_ARCHITECTURE = "GeneralizedRCNN"
    cfg.BACKBONE.NAME = "build_convnext_backbone"
    cfg.MODEL.RPN.PRE_NMS_TOPK_TEST = 6000
    cfg.MODEL.RPN.POST_NMS_TOPK_TEST = 1000
    cfg.MODEL.ROI_HEADS.NAME = "Res5ROIHeads"
    # Optimizer configs
    cfg.SOLVER.IMS_PER_BATCH = 16
    cfg.SOLVER.BASE_LR = 0.02
    cfg.SOLVER.STEPS = (60000, 80000)
    cfg.SOLVER.MAX_ITER = 90000


def faster_rcnn_sift_resnet_config(cfg, args):
    raise NotImplementedError


def faster_rcnn_sift_convnext_config(cfg, args):
    raise NotImplementedError


def faster_rcnn_bar_resnet_config(cfg, args):
    raise NotImplementedError


def faster_rcnn_bar_convnext_config(cfg, args):
    raise NotImplementedError