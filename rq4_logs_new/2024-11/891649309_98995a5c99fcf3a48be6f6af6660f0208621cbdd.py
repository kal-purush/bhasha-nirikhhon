
from utils.node_utils import NodeBase
from transformers import AutoProcessor, SiglipVisionModel

class SigLIPLoader(NodeBase):
    def execute(self, model_id, dtype, device):
        model = SiglipVisionModel.from_pretrained(model_id, torch_dtype=dtype)
        processor = AutoProcessor.from_pretrained(model_id)

        model.eval() # TODO: is this necessary? I don't see it used anywhere but the guys at InstantX do it

        return { 'siglip_encoders': { 'processor': processor, 'model': model, 'device': device } }