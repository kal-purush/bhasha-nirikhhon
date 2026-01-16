# from wav2spec_stand_alone import wav2spec
from NsfHiFiGAN.nsf_hifigan import NsfHifiGAN
from CREPE.crepe import get_pitch_crepe
from temp_hparams import hparams, rel2abs

wav, mel = NsfHifiGAN.wav2spec(rel2abs(hparams['relative_raw_wave_path']), hparams)
gt_f0, coarse_f0 = get_pitch_crepe(wav, mel, hparams)
print(wav.shape, mel.shape)
print(gt_f0.shape, coarse_f0.shape)