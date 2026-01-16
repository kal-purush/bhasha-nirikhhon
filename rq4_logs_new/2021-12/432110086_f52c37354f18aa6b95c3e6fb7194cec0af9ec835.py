import json
from collections import Counter
from pathlib import Path
from typing import Union

import arabic_reshaper
import matplotlib.pyplot as plt
from bidi.algorithm import get_display
from hazm import Normalizer, word_tokenize
from loguru import logger
from src.data import DATA_DIR
from wordcloud import WordCloud


class ChatStatistics:
    """Generating chat statistics from a telegram chat json file
    """
    def __init__(self, chat_json: Union[str, Path]):
        """Generates a word cloud from the chat data 
         
        Args:
            chat_json : path to telegram export json file
        """
        #load chat data
        logger.info(f"Loading chat data from {chat_json}")
        with open(chat_json) as f:
            self.chat_data=json.load(f)

        #load stopwords    
        self.normalizer = Normalizer()
        logger.info(f"Loading stopwords data from {DATA_DIR / 'stop_words.txt'}")
        self.stop_words= open(DATA_DIR / 'stop_words.txt').readlines()
        self.stop_words= list(map(str.strip,self.stop_words))
        self.stop_words= list(map(self.normalizer.normalize, self.stop_words))

    def generate_word_cloud(self, output_dir :Union [str, Path]):
        """Generates a word cloud from the chat data 

        Args:
            output_dir : path to output directory for word cloud image
        """
        logger.info(f"Generating word cloud...")
        text_content=''
        for msg in self.chat_data['messages']:
            if type(msg['text']) is str:
                tokens= word_tokenize(msg['text'])
                tokens = list(filter(lambda item:  item not in self.stop_words,tokens))
                text_content += f"{' '.join(tokens)}"
        
        text_content=self.normalizer.normalize(text_content)
        text_content = arabic_reshaper.reshape(text_content)
        
        text_content = get_display(text_content)
        text_content = arabic_reshaper.reshape(text_content)
        text_content = get_display(text_content)

        logger.info(f"saving word cloud to {output_dir}")
        wordcloud=WordCloud(
        width=1200,
        height=1200,
        font_path=str(DATA_DIR / './BHoma.ttf'),
        max_font_size=200,
        background_color='white'
        ).generate(text_content)
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis("off")
        wordcloud.to_file(str(Path(output_dir) / 'wordcloud.png'))
if __name__== "__main__":
    chat_stats = ChatStatistics(chat_json=DATA_DIR / 'online.json')
    chat_stats.generate_word_cloud(output_dir=DATA_DIR)

print('Done!')