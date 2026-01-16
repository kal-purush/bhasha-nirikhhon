from collections import defaultdict
    

class BytePairTrainer:
    """
    Trains a Byte Pair Encoding from a list of strings, in which words are
    assumed to be whitespace-separated.
    
    Example usage:
        lines = ['the dog doo', 'that dog they dogged']
        trainer = BytePairTrainer(lines)
        encoder = trainer.train(num_merges = 5)
        
    The resulting value *encoder* is an instance of a BytePairEncoder.

    """
    
    
    def __init__(self, lines):
        self.lines = list(lines)
        self.reset()
        
    def reset(self):
        self.tokens = []
        for line in self.lines:
            for word in line.split():
                letters = list(word)
                self.tokens.append(letters[:-1] + ['{}</w>'.format(letters[-1])])
            self.tokens.append([])
        self.pair_locations = defaultdict(list)
        for i, tok in enumerate(self.tokens):
            pairs = [(tok[i],tok[i+1]) for i in range(len(tok)-1)]
            for pair in pairs:
                self.pair_locations[pair].append(i)

    def train(self, num_merges):
        """
        Trains a BytePairEncoder for a specified number of merges.
        
        """          
        def most_common_pair():
            result = (None, 0)
            for key in self.pair_locations:
                num_locations = len(self.pair_locations[key])
                if num_locations > result[1]:
                    result = (key, num_locations)
            return result[0]
        self.reset()
        merges = []
        for i in range(num_merges):
            pair = most_common_pair()
            if pair is not None:
                merges.append((pair[0], pair[1]))
                self.merge(pair[0], pair[1])
            else:
                break
        return BytePairEncoder(merges)
    
    
    def merge(self, byte1, byte2):
        """
        Merges two bytes.
        
        """        
        def find_pair(byte1, byte2, token):
            i = 0
            while (i + 1 < len(token) and 
                   not (token[i] == byte1 and token[i+1] == byte2)):
                i += 1
            if i + 1 >= len(token):
                return None
            else:
                return i
        
        def merge_adjacent(ls, i):
            return ls[:i] + [ls[i] + ls[i+1]] + ls[i+2:]

        merged_byte = byte1 + byte2
        locs = [loc for loc in self.pair_locations[(byte1, byte2)]]
        for loc in locs:
            tok = self.tokens[loc] 
            byte1_index = find_pair(byte1, byte2, tok)
            disrupted = []
            discovered = []
            disrupted.append((tok[byte1_index], tok[byte1_index+1]))
            if byte1_index > 0:
                disrupted.append((tok[byte1_index-1], tok[byte1_index]))
                discovered.append((tok[byte1_index-1], merged_byte))
            if byte1_index + 2 < len(tok):
                disrupted.append((tok[byte1_index+1], tok[byte1_index+2]))
                discovered.append((merged_byte, tok[byte1_index+2]))
            for pair in disrupted:
                self.pair_locations[pair].remove(loc)
                if len(self.pair_locations[pair]) == 0:
                    del self.pair_locations[pair]
            for pair in discovered:
                self.pair_locations[pair].append(loc)
            self.tokens[loc] = merge_adjacent(tok, byte1_index)
            
                
class BytePairEncoder:
    """
    Implements Byte Pair Encoding, given a prioritized list of tokens to
    merge.
    
    Example usage:
        encoder = BytePairEncoder([('d', 'o'), ('t', 'h'), ('do', 'g</w>')])
        encoder.encode(['the dog doo', 'that dog they dogged'])
    
    """
    
    def __init__(self, merges):
        self.merges = merges
        
    def encode(self, lines):
        trainer = BytePairTrainer(lines)
        for (byte1, byte2) in self.merges:
            trainer.merge(byte1, byte2)
        result = ""
        for token in trainer.tokens:
            if len(token) == 0:
                result = result.strip()
                result += '\n'
            for i, wordpiece in enumerate(token):
                if i < len(token) - 1:
                    result += wordpiece + "&& "
                else:
                    result += wordpiece.replace('</w>', ' ')
        return result

           
            