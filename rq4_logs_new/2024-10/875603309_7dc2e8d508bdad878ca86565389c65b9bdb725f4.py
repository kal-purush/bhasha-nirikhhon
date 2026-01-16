import nltk
import sys
nltk.download('stopwords')
from nltk.corpus import stopwords

en_stopwords = stopwords.words('english')

def tokenize_stop(text):
    no_stop = ' '.join([word for word in text.split() if word not in (en_stopwords)])
    return no_stop

if __name__ == "__main__":
    input_text = sys.argv[1]
    tokens = tokenize_stop(input_text)
    print(tokens)