from tokenizers import Tokenizer
from tokenizers.models import BPE
from tokenizers.trainers import BpeTrainer
from tokenizers.pre_tokenizers import Whitespace
from tokenizers.processors import TemplateProcessing

WINDOW_SIZE = 64

def read_data(file_name):
  """
  DO NOT CHANGE
  Load text data from file
  :param file_name:  string, name of data file
  :return: list of sentences, each a list of words split on whitespace
  """
  lines = []
  char = []
  with open(file_name, 'rt', encoding='latin',newline="") as data_file:
    for line in data_file:
      if line[0] != '[' and line[0] != '(' and "THE ONE" not in line and "Written by:" not in line:
        line_text = line.rstrip('\n').split(": ", 1)
        if len(line_text) == 2:
          char.append(line_text[0])
          lines.append(line_text[1])
  return char, lines

def get_data(file_name):
  chars, lines = read_data(file_name)

  tokenizer = Tokenizer(BPE(unk_token="[UNK]"))
  tokenizer.enable_padding(direction="right", pad_id=4,
                                  pad_token='[PAD]',
                                  length=WINDOW_SIZE+1)
  trainer = BpeTrainer(special_tokens=["[UNK]", "[BOS]", "[EOS]", "[DELIM]", "[PAD]"])
  tokenizer.pre_tokenizer = Whitespace()
  tokenizer.train([file_name], trainer)
  tokenizer.post_processor = TemplateProcessing(
      single="[BOS] $A [EOS]",
      pair="[BOS] $A [DELIM] $B:1 [EOS]:1",
      special_tokens=[
          ("[BOS]", tokenizer.token_to_id("[BOS]")),
          ("[EOS]", tokenizer.token_to_id("[EOS]")),
          ("[DELIM]", tokenizer.token_to_id("[DELIM]")),
      ],
  )

  all_tokens = []
  all_ids = []
  
  for i in range(len(lines)): 
    output = tokenizer.encode(chars[i], lines[i])
    all_tokens.append(output.tokens)
    all_ids.append(output.ids)

  

get_data("../data/friends_transcript.txt")
    
    



