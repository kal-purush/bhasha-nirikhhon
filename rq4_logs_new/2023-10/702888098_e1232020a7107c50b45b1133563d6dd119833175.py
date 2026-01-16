import argparse
import random
from numpy import random as rd
import copy 
from queue import PriorityQueue
from tqdm import tqdm 
from pathlib import Path

kb = 1000 # kilobyte
rho = None # speed of light delay
Mbps = 1000*1000 # value of Mbps
kbbits = 1000 # value of kbbits
interarrivalTime = 600 # seconds
miningFee = 50 
noOfTransactions = 0 # total number of transaction taken place
noOfBlocks = 0 # total number of blocks formed

# Transaction class describes a single transaction
class Transaction:
    
    size = 1 * kb

    def __str__(self):
        return str({ 'txnID': self.txnID, 'idx': self.idx, 'idy': self.idy, 'coins': self.coins}) 

    def __init__(self, idx, idy, coins):

        # updating total number of transactions generated
        global noOfTransactions
        noOfTransactions = noOfTransactions + 1
        self.txnID = noOfTransactions 
        self.idx = idx # sender
        self.idy = idy # receiver
        self.coins = coins # amount of coins to be transferred


# Block class describes a single block mined by peers
class Block:
    blkID : int # unique ID describing a block
    transactionList : list[Transaction] # list of transactions to be stored in the block

    # helper function to print the block details
    def __str__(self):
        return str(self.blkID) + ", " + (str(self.prevBlock.blkID) if self.prevBlock else str(0)) + ", " + (str(self.transactionList[0].idy) if len(self.transactionList) else str(-1))
        # return str(self.blkID) + "->" + (str(self.prevBlock.blkID) if self.prevBlock else str(0)) + "-".join([str(tx) for tx in self.transactionList])

    def store_info(self):
        f = open("block_data.txt", "a")
        f.write(str(self.blkID) + "->" + (str(self.prevBlock.blkID) if self.prevBlock else str(0)) + str([str(tx) for tx in self.transactionList]) + "\n")
        f.close()

    def __init__(self, transactionList: list, prevBlock):
        # updating total number of blocks generated
        global noOfBlocks
        noOfBlocks = noOfBlocks + 1
        self.blkID = noOfBlocks

        self.transactionList = transactionList # list of transactions to be stored in the block
        self.prevBlock = prevBlock # The previous block in the chain the block is mined on
        self.store_info()


# Peer class describes a single peer in the network
class Peer:
    connectedPeers :list # adjacent peers in network
    transactionList : list[Transaction] # contains entire transactions received by peer
    blockchain : list[Block] # tree of blockchain 
    timelist : list[float] # time of reception/generation of block
    longestChain: Block # terminal block of longest chain
    longestChainLength: int # length of longest chain

    selfish: bool
    stubborn: bool
    hiddenChain: list[Block]
    lead: int
    generatedAdvBlock: int

    def __str__(self):
        return "\n".join([ str(block) + ", " + str(self.timelist[idx]) for idx, block in enumerate(self.blockchain)])

    def printLongestChain(self):
        res = ""
        blk = self.longestChain
        while(blk):
            res += str(blk.blkID) + ", "
            blk = blk.prevBlock
        return res

    # helper function to check if a transaction is present in any other block
    def uniqueTransaction(self, transaction: Transaction):
        block = copy.deepcopy(self.longestChain)
        while (block):
            for tx in block.transactionList:
                if transaction.txnID == tx.txnID:
                    return False
            block = block.prevBlock
        return True

    # to generate a block to be mined from the pending Transactions
    def generateBlock(self):
        txList = []
        txList.append(Transaction(None, self.uID, miningFee)) # coinbase transaction
        balance = {}

        # calculation of balance available of all peers
        for id in range(self.numPeers):
            balance[id] = self.calculateBalance(id)

        # adding transactions to the block and updating balance
        for transaction in self.transactionList:
            if self.uniqueTransaction(transaction) and balance[transaction.idx] >= transaction.coins:
                balance[transaction.idx] -= transaction.coins
                balance[transaction.idy] += transaction.coins
                txList.append(transaction)
            if len(txList) == 999:
                break 
        
        # generating block
        block = Block(txList, self.longestChain)
        return block

    def __init__(self, uID, speed, cpu, hashingFraction, genesisBlock, ttx, numPeers, selfish, stubborn):
        self.connectedPeers = [] # adjacent peers in network
        self.transactionList = [] # contains entire transactions received by peer
        self.blockchain = [] # tree of blockchain
        self.uID = uID # unique ID of peer
        self.speed = speed # network speed
        self.cpu = cpu # cpu speed :: mining
        self.blockchain.append(genesisBlock) # genesis block
        self.longestChain = genesisBlock
        self.longestChainLength = 1
        self.numPeers = numPeers
        self.ttx = ttx # mean of inter-arrival time of transactions
        self.timelist = [] # time of reception/generation of block
        self.timelist.append(0)
        
        self.selfish = selfish
        self.stubborn = stubborn
        self.hiddenChain = []
        self.lead = 0
        self.generatedAdvBlock = 0

        # generating first mine for self generation
        self.hashingFraction = hashingFraction

        # generating first mining time for generation block
        miningTime = rd.exponential(scale=interarrivalTime/self.hashingFraction, size=(1, 1))[0][0]
        # setting time for first transaction
        txTime = rd.exponential(scale=self.ttx, size=(1, 1))[0][0]
        
        # adding first mining and transaction to queue
        global eventQueue
        eventQueue.put((miningTime, Event(0+miningTime, True, False, None, self, self.generateBlock())))        
        eventQueue.put((txTime, Event(0+txTime, False, False, None, self, None)))    

    # return number of blocks in the longest chain mined by the peer
    def calculateSelfBlocksInLongestChain(self):
        num = 0
        block = copy.deepcopy(self.longestChain)
        while (block):
            if block.transactionList[0].idy == self.uID:
                num+=1
            block = block.prevBlock 
        return num

    # return number of blocks mined by the peer
    def calculateSelfBlocks(self):
        num = 0
        for block in self.blockchain:
            if block.transactionList[0].idy == self.uID:
                num+=1
        return num

    # function to check if transaxtion violates constraint of balance
    def checkTransactionValidity(self, transaction: Transaction):
        sender = transaction.idx
        coins = transaction.coins
        presentBalance = 0
        
        # transactions in the longest chain
        for chain in self.blockchain:
            for transaction in chain.transactionList:
                if transaction.idx == sender:
                    presentBalance -= transaction.coins
                elif transaction.idy == sender:
                    presentBalance += transaction.coins
        
        # transactions in the pending list
        for transaction in self.transactionList:
            if transaction.idx == sender:
                    presentBalance -= transaction.coins
            elif transaction.idy == sender:
                presentBalance += transaction.coins
        
        # constraint check
        if coins > presentBalance:
            return False

    # function to handle transaction received by peer
    def handleTransactionReceive(self, transaction: Transaction, time: float, sendPeer):
        # if transaction received dont process it 
        if transaction in self.transactionList:
            return 

        self.transactionList.append(transaction) # add transaction to pending list

        # broadvasting transaction to all peers
        for peer in self.connectedPeers:
            # if peer is the sender dont send it back
            if peer.uID is sendPeer.uID:
                continue
            
            # link speed
            c = 100 * Mbps 
            if peer.speed == "slow" or self.speed == "slow":
                c = 5 * Mbps

            # queueing delay at peer
            d = rd.exponential(scale=(96 * kbbits/c), size=(1, 1))[0][0]

            # latency
            latency = rho[self.uID][peer.uID] + transaction.size/c + d

            # adding event to queue
            eventQueue.put((time + latency, Event(time + latency, False, True, self, peer, transaction)))

    # helper function to calculate the bitcoins avialable to a peer from it's own blockchain
    def calculateBalance(self, uID):
        balance = 0
        block = copy.deepcopy(self.longestChain)
        while (block):
            for transaction in block.transactionList:
                if transaction.idx == uID:
                    balance -= transaction.coins 
                if transaction.idy == uID:
                    balance += transaction.coins 
            block = block.prevBlock
        return balance
    
    # generating transaction
    def handleTransactionGeneration(self, time: float):
        idx = self.uID
        # Choose random idy
        idy = idx
        while idy == idx:
            idy = random.sample(list(range(0, self.numPeers)), 1)[0]
        # Random amount of coins will be put based on balance of idx
        balance = self.calculateBalance(self.uID)
        coins = random.randint(0, balance) 
        transaction = Transaction(idx, idy, coins)

        self.transactionList.append(transaction)
        # Broadcast transaction to all peers
        for peer in self.connectedPeers:

            # Link speed
            c = 100 * Mbps
            if peer.speed == "slow" or self.speed == "slow":
                c = 5 * Mbps
            # Queueing delay at peer
            d = rd.exponential(scale=(96 * kbbits/c), size=(1, 1))[0][0]
            #   Latency
            latency = rho[self.uID][peer.uID] + transaction.size/c + d
            #  Adding event to queue
            eventQueue.put((time + latency, Event(time + latency, False, True, self, peer, transaction)))

        # Generate another transaction
        txTime = rd.exponential(scale=self.ttx, size=(1, 1))[0][0]
        eventQueue.put((time + txTime, Event(time + txTime, False, False, None, self, None)))
        
    # check validity of block
    def checkBlockValidity(self, blk: Block):
        # check if block is already present in blockchain
        if blk in self.blockchain:
            return 0 
        balance = {}
        for id in range(self.numPeers):
            balance[id] = 0
        count = 0  # count of blocks in longest chain
        block = copy.deepcopy(blk)
        # calculate balance of all peers in blockchain including this block
        while(block):
            for transaction in block.transactionList:
                if transaction.idx:
                    balance[transaction.idx] -= transaction.coins  
                balance[transaction.idy] += transaction.coins  
            block = block.prevBlock 
            count += 1
        # check if balance is negative
        for key in balance:
            if balance[key] < 0:
                return 0
        return count
    
    def broadcastBlock(self, block: Block, time: int):
         # broadcast to other nodes
        block_length = kbbits * (1 + len(block.transactionList))
        # broadcast to other nodes
        for peer in self.connectedPeers:
            # link speed
            c = 100 * Mbps
            if peer.speed == "slow" or self.speed == "slow":
                c = 5 * Mbps
            # queueing delay at peer
            d = rd.exponential(scale=(96 * kbbits/c), size=(1, 1))[0][0]
            # latency
            latency = rho[self.uID][peer.uID] + block_length/c + d
            # adding event to queue
            eventQueue.put((time + latency, Event(time + latency, True, True, self, peer, block))) 

    def broadcast2Blocks(self, block1: Block, block2: Block, time: int):
         # broadcast to other nodes
        block1_length = kbbits * (1 + len(block1.transactionList))
        block2_length = kbbits * (1 + len(block2.transactionList))
        # broadcast to other nodes
        for peer in self.connectedPeers:
            # link speed
            c = 100 * Mbps
            if peer.speed == "slow" or self.speed == "slow":
                c = 5 * Mbps
            # queueing delay at peer
            d = rd.exponential(scale=(96 * kbbits/c), size=(1, 1))[0][0]
            # latency
            latency1 = rho[self.uID][peer.uID] + block1_length/c + d
            latency2 = rho[self.uID][peer.uID] + block2_length/c + d
            # adding event to queue
            eventQueue.put((time + latency1, Event(time + latency1, True, True, self, peer, block1))) 
            eventQueue.put((time + latency1 + latency2, Event(time + latency1 + latency2, True, True, self, peer, block2))) 


    # function to handle block received by peer
    def handleBlockReceive(self, block: Block, time: float, sendPeer):
        
        length = self.checkBlockValidity(block)
        if length == 0:
            return
        self.blockchain.append(block) 
        self.timelist.append(time)

        # Selfish or Stubborn miner
        if self.selfish or self.stubborn:
            # State 0' - Adversary mines followed by a block by honest miners
            if (self.lead == -1):
                self.lead = 0
                if length > self.longestChainLength:
                    self.longestChainLength = length
                    self.longestChain = block 
                    miningTime = rd.exponential(scale=interarrivalTime/self.hashingFraction, size=(1, 1))[0][0] # time to mine a block
                    eventQueue.put((time+miningTime, Event(time+miningTime, True, False, None, self, self.generateBlock()))) # add event to queue
            
            # State 0 - Lead = 0 Adversary and honest miners mine on the same block
            elif (self.lead == 0):
                if length > self.longestChainLength:
                    self.longestChainLength = length
                    self.longestChain = block 
                    miningTime = rd.exponential(scale=interarrivalTime/self.hashingFraction, size=(1, 1))[0][0] # time to mine a block
                    eventQueue.put((time+miningTime, Event(time+miningTime, True, False, None, self, self.generateBlock()))) # add event to queue
                    
            # State 1 : Lead = 1 : adversary broadcasts private block
            elif (self.lead == 1):
                block = self.hiddenChain.pop(0)
                self.broadcastBlock(block, time)
                self.lead = -1

            # State 2 : Lead = 2 : selfish miner broadcast both the private blocks while stubborn miner just broadcast only one of them.
            elif (self.lead == 2):
                if self.stubborn:
                    while len(self.hiddenChain) > 1:
                        self.broadcastBlock(self.hiddenChain.pop(0), time)
                    self.lead = 1
                else:
                    self.broadcast2Blocks(self.hiddenChain.pop(0), self.hiddenChain.pop(0), time)
                    self.lead = 0

            # Lead > 2 : Broadcast a single block to compete with the honest chain
            elif (self.lead > 2):
                block = self.hiddenChain.pop(0)
                self.broadcastBlock(block, time)
                self.lead -= 1
            return

        # if block forms the longest chain
        if length > self.longestChainLength:
            self.longestChainLength = length
            self.longestChain = block 
            miningTime = rd.exponential(scale=interarrivalTime/self.hashingFraction, size=(1, 1))[0][0] # time to mine a block
            eventQueue.put((time+miningTime, Event(time+miningTime, True, False, None, self, self.generateBlock()))) # add event to queue
        
        self.broadcastBlock(block, time)
            
    # function to handle block generation by peer
    def handleBlockGeneration(self, block: Block, time: float):
        # if block when mined completely does not form the longest chain, then cancel the generation
        if (self.longestChain != block.prevBlock):
            return

        self.blockchain.append(block)
        self.timelist.append(time)
        self.longestChain = block
        self.longestChainLength += 1

        # generate another block mining event 
        miningTime = rd.exponential(scale=interarrivalTime/self.hashingFraction, size=(1, 1))[0][0]
        # add event to queue
        eventQueue.put((time+miningTime, Event(time+miningTime, True, False, None, self, self.generateBlock())))   

        # if selfish 
        if self.selfish:
            # if in state 0', i.e. some people are competing to form on attackers also
            # then if generated attacker gains by 2, revealing all his blocks, going to lead = 0
            self.generatedAdvBlock += 1
            if self.lead == -1:
                self.lead = 0 
                self.broadcastBlock(block, time)  
                return  
            self.lead += 1
            self.hiddenChain.append(block)
            return
        
        # if stubborn adversary generates first when in level = -1, then it keeps it secret
        # and goes to level = 1, continuing earlier mechanism
        if self.stubborn:
            self.generatedAdvBlock += 1
            if self.lead == -1:
                self.hiddenChain.append(block)
                self.lead = 1
                return 
            self.lead += 1
            self.hiddenChain.append(block)
            return
        
        self.broadcastBlock(block, time)    
    
               
# Event class to store all the upcoming events
class Event:
    def __init__(self, time, isBlock: bool, isReceive: bool, sendPeer: Peer, receivePeer: Peer, transactionOrBlockObject):
        self.time = time # time at which event occurs
        self.isBlock = isBlock # is event a block or transaction
        self.isReceive = isReceive # is event a receive or generation
        self.sendPeer = sendPeer # peer from which event is received
        self.receivePeer = receivePeer # peer to which event is sent
        self.transactionOrBlock = transactionOrBlockObject # contains transaction/ block object

    def simulate(self):
        if self.isBlock:
            if self.isReceive:
                self.receivePeer.handleBlockReceive(self.transactionOrBlock, self.time, self.sendPeer)
            else:
                self.receivePeer.handleBlockGeneration(self.transactionOrBlock, self.time)
        else:
            if self.isReceive:
                self.receivePeer.handleTransactionReceive(self.transactionOrBlock, self.time, self.sendPeer)
            else:
                self.receivePeer.handleTransactionGeneration(self.time)

# Network class to store all the peers
class Network:
    def __init__(self, peerList):
        self.peerList = peerList

    # depth first search to check if network is connected
    def dfs(self, v: Peer, visited):
        visited[v] = True
        for u in v.connectedPeers:
            if not visited[u]:
                self.dfs(u, visited)

    # function to check if network is connected
    def isConnected(self):
        visited={peer: False for peer in self.peerList}
        self.dfs(visited[self.peerList[0]], visited)

        for peer in self.peerList:
            if not visited[peer]:
                return False
        return True

# function to generate network of Peers
def generateNetwork(peerList: list[Peer], nbrsAttacker: int):
    for peer in peerList:   
        peer.connectedPeers = []

    remainingPeers = []
    for peer in peerList:
        remainingPeers.append(peer)

    # extablish connections between peers within 4 to 8 connections
    # We keep track if existing connecions and reduce it to as we come to a peer with existing connections
    for idx, peer in enumerate(peerList):
        if not peer in remainingPeers:
            continue
        try:
            if (idx == 0):
                connList: list[Peer] = list(random.sample(remainingPeers, nbrsAttacker))    
            else:
                connList: list[Peer] = list(random.sample(remainingPeers, min(random.randint(4,8)-len(peer.connectedPeers), len(remainingPeers))))
            peer.connectedPeers.extend(connList)
            for p in connList:
                p.connectedPeers.append(peer)
        except ValueError as error:
            return generateNetwork(peerList, nbrsAttacker)

        # remove peers with 8 connections
        peersToRemove = []
        for p in remainingPeers:
            if len(p.connectedPeers) == 8:
                peersToRemove.append(p)
        for p in peersToRemove:
            remainingPeers.remove(p)

    network = Network(peerList)
    if network.isConnected:
        # for peer in peerList:
        #     print(peer.uID)
        # for peer in peerList:
        #     for p in peer.connectedPeers:
        #         print(peer.uID, p.uID) d
        return network
    else:
        generateNetwork(peerList, nbrsAttacker)
        

if __name__ == '__main__':
    random.seed(1)

    parser = argparse.ArgumentParser()
    parser.add_argument('--peers', type=int, required=True)
    parser.add_argument('--z0', type=int, required=True)
    parser.add_argument('--z1', type=int, required=True)
    parser.add_argument('--ttx', type=int, required=True)
    parser.add_argument('--hf', type=int, required=True)
    parser.add_argument('--tau', type=float, required=True)
    parser.add_argument('--attack', type=str, required=True)
    
    args = parser.parse_args()
    slowTx = random.sample(list(range(args.peers)), int(args.peers * args.z0 / 100))
    slowCPU = random.sample(list(range(args.peers)), int(args.peers * args.z1 / 100))


    peerList: list[Peer] = [] # list of peers

    # delay of propagation of block from peer i to peer j
    rho = [[0]*args.peers]*args.peers
    for i in range(args.peers):
        for j in range(i + 1, args.peers):
            rho[j][i] = rd.uniform(low = 10 * 1e-3, high = 500 * 1e-3, size=(1, 1))[0][0]

     # number of slow CPU peers
    slowCPUnumber = len(slowCPU)

    # generate genesis block
    genesisBlock = Block([Transaction(0, 0, 0)], None)

    eventQueue = PriorityQueue()

    # hashing fraction
    for i in range(args.peers):
        speed = 'slow' if i in slowTx else 'fast'
        cpu = 'slow' if i in slowCPU else 'fast'
        if 0 in slowCPU:
            hashingFraction = (1/(10*(args.peers-1)-9*(slowCPUnumber-1))*(1-args.hf/100) if cpu == 'slow' else 10/(10*(args.peers-1)-9*(slowCPUnumber-1)))*(1-args.hf/100)
        else:
            hashingFraction = (1/(10*(args.peers-1)-9*(slowCPUnumber))*(1-args.hf/100) if cpu == 'slow' else 10/(10*(args.peers-1)-9*(slowCPUnumber)))*(1-args.hf/100)

        if i==0:
            if args.attack == "selfish":
                peerList.append(Peer(i, speed, cpu, args.hf/100, genesisBlock, args.ttx, args.peers, True, False))
            elif args.attack == "stubborn":
                peerList.append(Peer(i, speed, cpu, args.hf/100, genesisBlock, args.ttx, args.peers, False, True))
        else:    
            peerList.append(Peer(i, speed, cpu, hashingFraction, genesisBlock, args.ttx, args.peers, False, False))

    random.shuffle(peerList[1:])
    generateNetwork(peerList, int(args.tau*args.peers/100)) # generate network of peers
    
    # simulation of all events
    for i in tqdm(range(10000)):
        time, event = eventQueue.get()
        event.simulate()


    mpu_node_adv = peerList[0].calculateSelfBlocksInLongestChain() / peerList[0].calculateSelfBlocks()
    mpu_node_overall = peerList[0].longestChainLength / noOfBlocks
    frac_att_main_chain = peerList[0].calculateSelfBlocksInLongestChain() / peerList[0].longestChainLength

    print(f"MPU node adv : {mpu_node_adv}")
    print(f"MPU node overall : {mpu_node_overall}")
    print(f"Fraction of attacker blocks in main chain : {frac_att_main_chain}")
    # print longest chain of each peer
    Path("./data").mkdir(parents=True, exist_ok=True)
    for idx, peer in enumerate(peerList):
        f = open("./data/treedata_" + str(idx) + ".txt", "w")
        f.write(peer.printLongestChain() + "\nnode, prevNode, creator, time\n" + peer.__str__())
        f.close()

    f = open("./data/peerdata.txt", "w")
    f.write("\n".join([str(peer.uID) + ", " + peer.speed + ", " + peer.cpu for peer in peerList]))

        