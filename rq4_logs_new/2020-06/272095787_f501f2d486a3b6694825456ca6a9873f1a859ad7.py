import spacy

nlp = spacy.load('en')


def tokenize(text: str) -> [str]:
    tokens = [token for token in nlp(text) if not token.is_stop]
    return [token.norm_ for token in tokens]