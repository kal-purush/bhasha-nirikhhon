import os
import hydra
import numpy as np
from omegaconf import DictConfig
from . import model as models


@hydra.main(config_path='conf', config_name='config')
def extract_embedding(cfg: DictConfig) -> None:
    # load embeddings
    audio_emb = np.load('{}/{}.npy'.format(cfg.train.embedding_dir, cfg.train.audio_alg))
    image_emb = np.load('{}/{}.npy'.format(cfg.train.embedding_dir, cfg.train.image_alg))

    # load model TODO: automatically get best checkpoint for model
    # for now just make sure it's the right model
    assert (
        '{}-{}'.format(cfg.train.audio_alg, cfg.train.image_alg) in
        os.path.basename(cfg.predict.model_ckpt)
    ), 'you passed an invalid model checkpoint. compare to your specified source embeddings.'
    
    baseline = models.Baseline.load_from_checkpoint(cfg.predict.model_ckpt)

    # predict embeddings
    audio_trans, image_trans = baseline.model(audio_emb, image_emb)

    # save audio embeddings
    np.save(
        os.path.join(
            cfg.predict.output_dir, 
            '{}_translated.npy'.format(cfg.train.audio_alg)),
        audio_trans)

    # save image embeddings
    np.save(
        os.path.join(
            cfg.predict.output_dir, 
            '{}_translated.npy'.format(cfg.train.image_alg)),
        image_trans)


if __name__ == '__main__':
    extract_embedding()