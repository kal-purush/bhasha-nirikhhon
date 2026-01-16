import os
import math
import torch
import torchaudio
import torchaudio.transforms as T
from vocos import Vocos
from omegaconf import OmegaConf
from accelerate import Accelerator
from diffusion import SpacedDiffusion, space_timesteps, get_named_beta_schedule
from aa_model import AA_diffusion, denormalize_tacotron_mel, normalize_tacotron_mel

import utils
from repcodec.whisper_feature_reader import WhisperFeatureReader
from repcodec.RepCodec import RepCodec


def padding_to_8(x):
    l = x.shape[-1]
    l = (math.floor(l / 8) + 1) * 8
    x = torch.nn.functional.pad(x, (0, l-x.shape[-1]))
    return x


def do_spectrogram_diffusion(diffusion_model, diffuser, latents, conditioning_latents, temperature=1, verbose=True):
    """
    Uses the specified diffusion model to convert discrete codes into a spectrogram.
    """
    with torch.no_grad():
        output_seq_len = latents.shape[2]
        output_shape = (latents.shape[0], 100, output_seq_len)

        noise = torch.randn(output_shape, device=latents.device) * temperature
        mel = diffuser.p_sample_loop(diffusion_model, output_shape, noise=noise,
                                    model_kwargs= {
                                    "hint": latents,
                                    "refer": conditioning_latents
                                    },
                                    progress=verbose)
        return denormalize_tacotron_mel(mel)[:,:,:output_seq_len]


class Inference:
    def __init__(self, cfg_path="config.yaml", checkpoint_path="models/ns2vc_hindi_156k.pt", repcodec_path="models/repcodec_hindi_200k_steps.pkl", device=None):
        self.cfg = OmegaConf.load(cfg_path)
        if device:
            self.device = device
        else:
            self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.accelerator = Accelerator()
        trained_diffusion_steps = 1000
        self.desired_diffusion_steps = 50
        cond_free_k = 2.
        self.infer_diffuser = SpacedDiffusion(use_timesteps=space_timesteps(trained_diffusion_steps, [self.desired_diffusion_steps]), model_mean_type='epsilon',
                           model_var_type='learned_range', loss_type='mse', betas=get_named_beta_schedule('linear', trained_diffusion_steps),
                           conditioning_free=True, conditioning_free_k=cond_free_k, sampler='dpm++2m')
        self.diffusion = AA_diffusion(self.cfg)
        self.vocos = Vocos.from_pretrained("charactr/vocos-mel-24khz")
        self.reader = None
        self.repcodec_path = repcodec_path
        self.repcodec = None
        self.load_diffusion_model(checkpoint_path)

    def load_diffusion_model(self, model_path):
        data = torch.load(model_path, map_location=self.accelerator.device)
        state_dict = data['model']
        self.model = self.accelerator.unwrap_model(self.diffusion)
        self.model.load_state_dict(state_dict)
        self.model.to(self.device)
        self.model.eval()

    def load_repcodec_model(self, repcodec_chkpt_path):
        whisper_root, whisper_name, layer = "~/whisper_model",  "medium", 24
        if self.reader is None:
            self.reader = WhisperFeatureReader(whisper_root, whisper_name, layer, device=self.device)
        self.repcodec_model = utils.get_repcodec_model(repcodec_chkpt_path, self.device).to(self.device)

    def wav_to_repcodec(self, wav, sr):
        if wav.shape[0] > 1:  # mix to mono
            wav = wav.mean(dim=0, keepdim=True)
        if sr == 16000:
            wav16k = wav
        else:
            wav16k = T.Resample(sr, 16000)(wav)
        whisper_feats = self.reader.get_feats_tensor(wav16k[0], rate=16000).unsqueeze(0).transpose(1, 2)
        with torch.no_grad():
            x = self.repcodec_model.encoder(whisper_feats)
            z = self.repcodec_model.projector(x)
        return z
    
    def wav_to_mel(self, wav, sr):
        if wav.shape[0] > 1:  # mix to mono
            wav = wav.mean(dim=0, keepdim=True)
        if sr == 24000:
            wav24k = wav
        else:
            wav24k = T.Resample(sr, 24000)(wav)
        spec_process = torchaudio.transforms.MelSpectrogram(
            sample_rate=24000,
            n_fft=1024,
            hop_length=256,
            n_mels=100,
            center=True,
            power=1,
        )
        spec = spec_process(wav24k)# 1 100 T
        spec = torch.log(torch.clip(spec, min=1e-7))
        return spec

    def infer(self, cvec, refer, temperature=0.8):
        cvec, refer = cvec.to(self.device), refer.to(self.device)
        refer_padded = normalize_tacotron_mel(refer)
        with torch.no_grad():
            mel = do_spectrogram_diffusion(self.model, self.infer_diffuser, cvec, refer_padded, temperature=temperature)
            mel = mel.detach().cpu()
        gen = self.vocos.decode(mel)
        return gen.cpu(), 24000

    def inference(self, original_audio, reference_audio, temperature=0.8, reference_cvec=None, save_path=None):
        og_wav, og_sr = torchaudio.load(original_audio)
        ref_wav, ref_sr = torchaudio.load(reference_audio)
        if reference_cvec is None:
            if self.repcodec is None:
                self.load_repcodec_model(self.repcodec_path)
            cvec = self.wav_to_repcodec(og_wav, og_sr)
        else:
            cvec = torch.load(reference_cvec, map_location=self.device)
        og_mel = self.wav_to_mel(og_wav, og_sr)
        cvec = utils.repeat_expand_2d(cvec.squeeze(0), og_mel.shape[-1]).unsqueeze(0)
        ref_mel = self.wav_to_mel(ref_wav, ref_sr)
        res_audio, res_sr = self.infer(cvec, ref_mel, temperature=temperature)
        if save_path:
            return torchaudio.save(save_path, res_audio, res_sr)
        else:
            return res_audio, res_sr


if __name__=="__main__":
    inference = Inference()