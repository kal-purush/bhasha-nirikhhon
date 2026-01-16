import datetime

import hashlib

import json


class BlockChain :

    def __init__(self):
        self.chain = []
        self.create_block(proof=1,previoushash='0')

    def create_block(self,proof,previoushash):
        block = {'index':len(self.chain)+1,
                'timestamp':str(datetime.datetime.now()),
                'proof':proof,
                'previous hash' : previoushash}
        self.chain.append(block)
        return block
    
    def print_previous_block(self):
        return self.chain[-1]
    
    def proof_of_work(self,previousproof):
        newProof = 1
        checkProof = False

        while checkProof is False:
            hashOperation = hashlib.sha256(
                str(newProof**2 - previousproof**2).encode()).hexdigest()
            
            if hashOperation[:5] == '0000':
                checkProof = True
            else:
                newProof += 1
            
            return newProof
    
    def hash(self,block):
        encodedBlock = json.dumps(block,sort_keys=True).encode()
        return hashlib.sha256(encodedBlock).hexdigest()
    
    def chain_valid(self,chain):
        previousBlock = chain[0]
        blockIndex = 1

        while blockIndex < len(chain):
            block = chain[blockIndex]
            if block['previous_hash'] != self.hash(previousBlock):
                return False
            
            previousProof = previousBlock['proof']
            proof = block['proof']
            hashOperation = hashlib.sha256(
                str(proof**2 - previousProof**2).encode()).hexdigest()
            
            if hashOperation[:5] != '0000':
                return False
            previousBlock = block
            blockIndex += 1

        return True
