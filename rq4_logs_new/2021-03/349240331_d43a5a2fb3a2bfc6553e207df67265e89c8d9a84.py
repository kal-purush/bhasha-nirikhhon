import hashlib

class Block:
    def __init__(self,previous_hash,transcation):
        self.transcation=transcation # its like this. in java
        self.previous_hash=previous_hash # smae thing for previous hash
        #unique digital fingerprint: transcation + previous blocks fingerprint
        string_to_hash="".join(transcation) + previous_hash
        self.block_hash=hashlib.sha256(string_to_hash.encode()).hexdigest()
