from dataclasses import dataclass
from dataclasses import field
from typing import List


@dataclass
class SourceSettings:
    origin: str
    author: int
    author_name: str
    guild: int
    guild_name: str
    channel: int
    channel_name: str


@dataclass
class EdenClipXConfig:
    text_input: str
    generator_name: str = "eden-clipx"
    image_url: str = ""
    step_multiplier: float = 1.0
    color_target_pixel_fraction: float = 0.75
    color_loss_f: float = 0.0
    color_rgb_target: tuple[float] = (0.0, 0.0, 0.0)
    image_weight: float = 0.35
    n_permuted_prompts_to_add: int = -1
    width: int = 0
    height: int = 0
    num_octaves: int = 3
    octave_scale: float = 2.0
    clip_model_options: List = field(
        default_factory=lambda: [["ViT-B/32", "ViT-B/16", "RN50"]],
    )
    num_iterations: tuple[int] = (100, 200, 300)


@dataclass
class StableDiffusionConfig:
    text_input: str
    width: int = 512
    height: int = 512
    ddim_steps: int = 166
    plms: bool = True
    C: int = 4
    f: int = 8
    upscale: bool = False
    generator_name: str = "stable-diffusion"


@dataclass
class OracleConfig:
    text_input: str
    generator_name: str = "oracle"