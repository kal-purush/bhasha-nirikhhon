from cgi import print_form
from datetime import datetime
import datetime as _dt
import hashlib as _hash
import json as _js
from pickle import TRUE
from shutil import which

class Blockchain:
    def __init__(self) -> None:
        self.chain=list()
        initial_block = self.register_user(
            name_of_seller="genesis block",id_of_property=0,nonce=1,previous_hash="0", index=1
        )
        self.chain.append(initial_block)
    def compute_nonce(self,current_nonce: int,name_of_seller: str,id_of_property: int,index: int)->bytes:
        hash_input=str(id_of_property**2-index**2+current_nonce)+name_of_seller
        return hash_input.encode()

# Proof of work algorithm computes takes a input string, which are names of the buyer and seller, computes a nounce which meets with the difficulty traget
#  of 4 leading zeroes
    def proof_of_work(self,name_of_seller: str,id_of_property: int,index: int) ->int:
        current_nonce=1
        condition=False
        while not condition:
            hash_in=self.compute_nonce(current_nonce,name_of_seller,id_of_property,index)
            hash_value=_hash.sha256(hash_in).hexdigest()
            if(hash_value[:4]=="0000"):
                condition=TRUE
            else:
                current_nonce=current_nonce+1
        return current_nonce

# Registers a user with the previously owned property 
    def mine_a_block(self,name_of_seller: str,id_of_property: int) ->dict:
        previous_block=self.get_previous_block()
        index=len(self.chain)
        nonce=self.proof_of_work(name_of_seller,id_of_property,index)
        previous_hash=previous_block["merkel_root"]
        if(self.verify_user(name_of_seller,id_of_property)==0):
            print("invalid property id")
            return
        block=self.register_user(name_of_seller=name_of_seller,id_of_property=id_of_property,nonce=nonce,previous_hash=previous_hash,index=index)
        return block
        
# Initiates the process of mining the block for buying and selling operation
    def mine_a_transaction(self,name_of_seller: str,name_of_buyer: str,id_of_property: int) ->dict:
        previous_block=self.get_previous_block()
        index=len(self.chain)
        str=name_of_buyer+name_of_seller
        nonce=self.proof_of_work(str,id_of_property,index)
        previous_hash=previous_block["merkel_root"]
        block=self.register_transaction(name_of_seller,name_of_buyer,id_of_property,nonce,previous_hash,index)
        return block

# Computes the hash value for the given block     
    def hash_function(self,block)->str:
        encoded_block=_js.dumps(block,sort_keys=True).encode()
        return _hash.sha256(encoded_block).hexdigest()

# Returns the last block
    def get_previous_block(self) ->dict:
        return self.chain[-1]

# Adds the block containing information about owner and his property 
    def register_user(self,name_of_seller: str,id_of_property: int,nonce: int,previous_hash: str,index: int) ->dict:
        block={"index":index,
                "name_of_seller":name_of_seller,
                "name_of_buyer":None,
                "id_of_property":id_of_property,
                "time_stamp":str(_dt.datetime.now()),
                "previous_hash": previous_hash,
                "index": index,
                "nonce":nonce
        }
        hash_input=self.compute_nonce(nonce,name_of_seller,id_of_property,index)
        block["merkel_root"]=_hash.sha256(hash_input).hexdigest()
        self.chain.append(block)
        return block

#Adds the block containing the information about buying and selling operations
    def register_transaction(self,name_of_seller: str,name_of_buyer: str,id_of_property: int,nonce: int,previous_hash: str,index: int) ->dict:
        block={"index":index,
                "name_of_seller":name_of_seller,
                "name_of_buyer":name_of_buyer,
                "id_of_property":id_of_property,
                "time_stamp":str(_dt.datetime.now()),
                "previous_hash": previous_hash,
                "index": index,
                "nonce":nonce
        }
        if(self.verify_buyer_seller(name_of_buyer,name_of_seller,id_of_property)==0):
            print('The transaction is invalid')
            return 
        st=name_of_seller+name_of_buyer
        hash_input=self.compute_nonce(nonce,st,id_of_property,index)
        block["merkel_root"]=_hash.sha256(hash_input).hexdigest()
        self.chain.append(block)
        return block

#Checks whether the buyer and seller have been registered in the blockchain and validates if the seller is the latest owner of the land which he is selling
    def verify_buyer_seller(self,buyer: str,seller: str,id_of_property: int)->bool :
        i=0
        f=0
        f1=0
        while(i<len(self.chain)):
            cur_block=self.chain[i]
            if(cur_block["name_of_buyer"]==buyer or cur_block["name_of_seller"]==buyer):
                f=1
            if(cur_block["name_of_seller"]==seller or cur_block["name_of_buyer"]==seller):
                f1=1
            i=i+1
        if f1 and f==0:
            return False
        i=len(self.chain)-1
        while i>=0:
            cur_block=self.chain[i]
            if(cur_block["id_of_property"]==id_of_property):
                if(cur_block["name_of_buyer"]!=None):
                    if cur_block["name_of_buyer"]==seller:
                        return True
                    else:
                        return False
                else:
                    if cur_block["name_of_seller"]==seller:
                        return True
                    else:
                        return False
            i=i-1
        return False

# Returns the transaction history of the property 
    def view_transaction_history(self,input_id: int)-> None :
        i=0
        flag=0
        while(i<len(self.chain)):
            b=self.chain[i]
            if(b["id_of_property"]==input_id):
                if(b["name_of_buyer"]!=None):
                    print(b["name_of_seller"],'sold the land to',b["name_of_buyer"],"on",b["time_stamp"])
                    flag=1
                else:
                    print('The Land Belongs To',b["name_of_seller"],b["time_stamp"])
                    flag=1
            i=i+1
        if(flag==0):
            print('Transaction history is not recorded for this land')

#####End of the Code######
            
            
    