const SHA256 = require('crypto-js/sha256');
const EC = require('elliptic').ec;
const ec = new EC('secp256k1');
const { MerkleTree } = require('merkletreejs');
const { PartitionedBloomFilter } = require('bloom-filters');
class Transaction {
  constructor(fromAddress, toAddress, amount) {
    this.fromAddress = fromAddress;
    this.toAddress = toAddress;
    this.amount = amount;
    this.timestamp = Date.now();
  }

  calculateHash() {
    return SHA256(this.fromAddress + this.toAddress + this.amount + this.timestamp).toString();
  }
  signTransaction(signingKey) {
    if (signingKey.getPublic('hex') !== this.fromAddress) {
      throw new Error('You cannot sign transaction for other wallets');
    }
    const hashTx = this.calculateHash();
    const sig = signingKey.sign(hashTx, 'base64');
    this.signature = sig.toDER('hex');
  }
  isValid() {
    if (this.fromAddress === null) return true;
    if (!this.signature || this.signature.length === 0) {
      throw new Error('No signature in this transaction');
    }
    const publicKey = ec.keyFromPublic(this.fromAddress, 'hex');
    return publicKey.verify(this.calculateHash(), this.signature);
  }
}

class Block {
  constructor(timestamp, transactions, previousHash = '') {
    this.previousHash = previousHash;
    this.timestamp = timestamp;
    this.transactions = transactions;
    this.hash = this.calculateHash();
    this.nonce = 0;

    const leaves = transactions.map(x => SHA256(x));
    this.tree = new MerkleTree(leaves, SHA256);

    this.filter = new PartitionedBloomFilter(120, 5, 0.5);

    for (const trx of this.transactions) {
      this.filter.add(trx.calculateHash());
    }
  }

  ifTransactionExists(transaction) {
    return this.filter.has(transaction.calculateHash());
  }

  calculateHash() {
    return SHA256(this.previousHash + this.timestamp + JSON.stringify(this.transactions) + this.nonce).toString();
  }
  mineBlock(difficulty) {
    while (this.hash.substring(0, difficulty) !== Array(difficulty + 1).join('0')) {
      this.nonce++;
      this.hash = this.calculateHash();
    }
    console.log('Block mined' + this.hash);
  }
  hasValidTransactions() {
    for (const tx of this.transactions) {
      if (!tx.isValid()) {
        return false;
      }
    }
    return true;
  }
}
class Blockchain {
  constructor() {
    this.chain = [this.createGenesisBlock()];
    this.difficulty = 2;
    this.pendingTransactions = [];
    this.miningReward = 10;
    this.burnMoney = 0;
    this.miningMoney = 0;
  }

  createGenesisBlock() {
    return new Block('01/01/2019', [], '0');
  }

  getLatestBlock() {
    return this.chain[this.chain.length - 1];
  }

  minePendingTransaction(miningRewardAddress) {
    let miningReward = this.miningReward;
    for (let i = 0; i < this.pendingTransactions.length; i++) {
      miningReward += this.chain.length + 1;
      this.burnMoney += this.chain.length;
      this.miningMoney += this.pendingTransactions[i].amount;
    }
    const rewardTx = new Transaction(null, miningRewardAddress, miningReward);
    this.pendingTransactions.push(rewardTx);
    let block = new Block(Date.now(), this.pendingTransactions, this.getLatestBlock().hash);
    block.mineBlock(this.difficulty);
    console.log('block succefully mined');
    this.chain.push(block);
    this.pendingTransactions = [];
  }
  getBalanceOfAddress(address) {
    let balance = 100;
    for (let i = 1; i < this.chain.length; i++) {
      for (const trans of this.chain[i].transactions) {
        if (trans.fromAddress === address) {
          balance -= trans.amount;
          balance -= i;
        }
        if (trans.toAddress === address) {
          balance += trans.amount;
        }
      }
    }
    return balance;
  }

  addTransaction(transaction) {
    if (!transaction.fromAddress || !transaction.toAddress) {
      throw new Error('Transaction must include from and to address');
    }
    if (!transaction.isValid()) {
      throw new Error('Cannot add invalide transaction to cahin');
    }
    if (this.getBalanceOfAddress(transaction.fromAddress) - transaction.amount - 1 - this.chain.length < 0)
      throw new Error('Not enough money in the wallet');

    this.pendingTransactions.push(transaction);
  }

  isChainValide() {
    for (let i = 1; i < this.chain.length; i++) {
      const currentBlock = this.chain[i];
      const previousBlock = this.chain[i - 1];
      if (!currentBlock.hasValidTransactions()) {
        return false;
      }

      if (currentBlock.hash !== currentBlock.calculateHash()) {
        return false;
      }
      if (currentBlock.previousHash !== previousBlock.hash) {
        return false;
      }
    }
    return true;
  }
  ifTransactionExistsBlockchain(transaction) {
    for (const block of this.chain) {
      if (block.ifTransactionExists(transaction)) {
        return true;
      }
    }
    return false;
  }
}

module.exports.Blockchain = Blockchain;
module.exports.Block = Block;
module.exports.Transaction = Transaction;