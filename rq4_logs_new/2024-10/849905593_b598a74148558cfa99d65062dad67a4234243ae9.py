import spacy
from config.config import CONFIG

spacy.require_gpu()
nlp = spacy.load(CONFIG.SPACY_MODEL_NAME, disable=['ner'])