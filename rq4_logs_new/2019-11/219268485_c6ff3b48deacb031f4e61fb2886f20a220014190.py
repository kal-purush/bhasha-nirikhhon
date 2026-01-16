# Module - 2 --  Building a Cryptocurrency

# To be installed:
# Flask==0.12.2  pip install Flask==0.12.2
# Postman HTTP client: https://www.postman.com/
# requests=2.18.4 : pip install requests==2.18.4

#importing libraries
import datetime
import json
import hashlib
from flask import Flask,jsonify, request
import requests                                     # used for consensus
from uuid import uuid4
from urllib.parse import urlparse

# part 1 -- Building Blockchain  ::::::::::::::::::::::::::::::::::::

class blockchain:
    
    def __init__(self):
        self.chain = []  # creating chain 
        self.transactions = []     # creating Transactions List
        
        #genesis Block
        self.create_block(nonce = 1 ,previous_hash = '0000' )
     
        # nodes
        self.nodes = set()
        
        
    def create_block(self,nonce,previous_hash):
        
        block = { 
                  'index' : len(self.chain)+1 ,
                  'time_stamp' : str(datetime.datetime.now()) ,
                  'nonce' : nonce ,
                  'previous_hash' : previous_hash,
                  'transactions' : self.transactions     # adding transactions in a block
                }
        
        self.transactions = []      # empty the list contaning transactions as they are assigned to the block....
        self.chain.append(block)
        return block
    
   
    
    def get_previous_block(self):
       
        return self.chain[-1]



    def proof_of_work(self,previous_nonce):
        new_nonce = 1
        check_nonce = False
        
        while check_nonce is False:
            hash_operation = hashlib.sha256(str(new_nonce**2 - previous_nonce**2).encode()).hexdigest()
            
            if hash_operation[:4] == '0000' :
                check_nonce = True
            else:
                new_nonce += 1
            
        return new_nonce
    
    def hash_of_block(self,block):

        encoded_block = json.dumps(block,sort_keys = True).encode()
        
        return hashlib.sha256(encoded_block).hexdigest()
        

    def is_chain_valid(self,chain):
        previous_block  = chain[0]
        block_index = 1
        
        while block_index < len(chain):
            current_block = chain[block_index]
            
            if current_block['previous_hash'] != self.hash_of_block(previous_block):
                print('this is false previous_hash')
                return False
            
            current_nonce = current_block['nonce']
            previous_nonce = previous_block['nonce']

            hash_operation = hashlib.sha256(str(current_nonce**2 - previous_nonce**2).encode()).hexdigest()
            
            if hash_operation[:4] != '0000':
                print('this is false nonce')
                return False
       
            previous_block = current_block
            block_index += 1 
        
        return True

# crypto code ///////////////////////////////////////////////////

    def add_transaction(self,sender,receiver,amount):          # this method adds transactions to transaction list 
        self.transactions.append({ 'sender' : sender,            # and returns the index of the block they are added to...
                                   'reciever' : receiver,
                                   'amount' : amount  })
        previous_block = self.get_previous_block()
        return previous_block['index']+1

    def add_node(self,address):
        parsed_url = urlparse(address)
        self.nodes.add(parsed_url.netloc)

    def replace_chain(self):
        network = self.nodes
        longest_chain = None
        max_length = len(self.chain)
        for node in network:
            response = requests.get(f'http://{node}/get_chain')    # this changes the address according to node to node...... 
            
            if response.status_code == 200:
                length = response.json()['length']
                chain = response.json()['chain']
                
                if length > max_length and self.is_chain_valid(chain):
                    max_length = length
                    longest_chain = chain 
        
        if longest_chain :
            self.chain = longest_chain
            return True
        return False
        
  
    
    
# mining a Block on Blockchain :::::::::::::
        
app = Flask(__name__)  

# Creating an address for the node on port 5000
node_address = str(uuid4()).replace('-','')

    
Blockchain = blockchain()   
 

@app.route('/mine_block',methods = ['GET'])

def mine_block():
    previous_block = Blockchain.get_previous_block()
    previous_nonce = previous_block['nonce']
    nonce = Blockchain.proof_of_work(previous_nonce)
    previous_hash = Blockchain.hash_of_block(previous_block)
    Blockchain.add_transaction(sender = node_address,receiver = 'rehan',amount = 1)
    
    block = Blockchain.create_block(nonce,previous_hash)
    
    response = { 
                 'message' : 'congrats!!!You have mined a block',
                 'index' : block['index'],
                 'time_stamp' : block['time_stamp'],
                 'nonce' : block['nonce'],
                 'previous_hash' : block['previous_hash'],
                 'transactions' : block['transactions']
                }
    return jsonify(response), 200



@app.route('/get_chain',methods = ['GET'])

def get_chain():
    
    response = { 'chain' : Blockchain.chain,
                 'Length' : len(Blockchain.chain)
                }
    
    return jsonify(response), 200

@app.route('/is_valid',methods = ['GET'])
    
def is_valid():
    
    is_valid = Blockchain.is_chain_valid(Blockchain.chain)
    
    response = { 'Blockchain is valid' : is_valid }
    
    return jsonify(response), 200


# Crypto code////////////////////////////////////////////////////



# Adding a new transactions to the blockchain

@app.route('/add_transactions',methods = ['POST'])    

def add_transactions():
    json = request.get_json()
    transaction_keys = ['sender','receiver','amount']
    if not all (key in json for key in transaction_keys):
        return 'some elements of the transaction are missing', 400
    index = Blockchain.add_transaction(json['sender'],json['receiver'],json['amount'])
    response = { 'message' : f'this transaction will be added to the block {index}' }
    
    return jsonify(response), 201 


# Part-3 Decentralizing our blockchain ::::::::::::::::::

# Connecting new nodes  
    
@app.route('/connect_node', methods = ['POST'])
def connect_node():
    json = request.get_json()
    nodes =  json.get('nodes')
    if nodes is None:
        return 'No node', 400
    for node in nodes:
        Blockchain.add_node(node)

    response = { 'message' : 'All nodes are now connected',
                 'total_nodes' : list(Blockchain.nodes)
                }

    return jsonify(response), 201

# Replacing the longest chain by the longest chain if needed

@app.route('/replace_chain',methods = ['GET'])    
def replace_chain():
    is_chain_replaced = Blockchain.replace_chain()
    if is_chain_replaced:
        response = {'message': 'the chain is replaced by longest one.......',
                    'new_chain' : Blockchain.chain
                    }
    else:
        response = {'message': 'All good,,,chain is longest one.......',
                    'new_chain' : Blockchain.chain
                    }

    return jsonify(response), 200


# running app 
    
app.run(host = '0.0.0.0',port = 5000)
    
    
    
    
    
    
    
    
    
    
  

 