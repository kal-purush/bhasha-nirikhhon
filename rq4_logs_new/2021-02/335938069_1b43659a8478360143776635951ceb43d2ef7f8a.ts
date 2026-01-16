import { shuffleArray } from './utils';

export type Question = {
  category: string;
  correct_answer: string;
  difficulty: string;
  incorrect_answers: string[];
  question: string;
  type: string;
}

export type QuestionState = Question & { answers: string[] };

export enum Difficulty {
  EASY = "easy",
  MEDIUM = "medium",
  HARD = "hard"
}
export const fetchQuestions = async (amount: number, difficulty: Difficulty) => {
  const endpoint = `https://opentdb.com/api.php?amount=${amount}&difficulty=${difficulty}&type=multiple`
  const data = await (await fetch(endpoint)).json();
  return data.results.map((question: QuestionState) => {
    return (
      {
        ...question,
        answers: shuffleArray([
          ...question.incorrect_answers, question.correct_answer
        ])
      }
    )
  })
}

export type Question_ = {
  correct_answer: string;
  answers: string[];
  question: string;
}
export const getQuestions = (): Question_[] => {
  return (
    [
      {
        correct_answer: "Peer to Peer",
        question: "What does P2P stand for?",
        answers: [
          "Password to Password",
          "Product to Product",
          "Peer to Peer",
          "Private Key to Public Key"
        ]
      },
      {
        correct_answer: "A computer on a Blockchain network",
        question: "What is a node?",
        answers: [
          "A computer on a Blockchain network",
          "A type of cryptocurrency",
          "An exchange",
          "A Blockchain"
        ]
      },
      {
        correct_answer: "Satoshi Nakamoto",
        question: "Who created Bitcoin?",
        answers: [
          "Samsung",
          "Satoshi Nakamoto",
          "John Mcafee",
          "China"
        ]
      },
      {
        correct_answer: "Wallet",
        question: "Where do you store your cryptocurrency?",
        answers: [
          "Bank account",
          "Floppy Disk",
          "Wallet",
          "In your pocket"
        ]
      },
      {
        correct_answer: "On an exchange",
        question: "Where is the LEAST SAFE place to keep your cryptocurrency?",
        answers: [
          "In your pocket",
          "On an exchange",
          "On a hot wallet",
          "At your work desk"
        ]
      },
      {
        correct_answer: "Computers that validate and process blockchain transactions",
        question: "What is a miner?",
        answers: [
          "A type of blockchain",
          "An algorithm that predicts the next part of the chain",
          "A person doing calculations to verify a transaction",
          "Computers that validate and process blockchain transactions"
        ]
      },
      {
        correct_answer: "All of the above",
        question: "What are the different types of tokens?",
        answers: [
          "Platform",
          "Privacy",
          "Currency",
          "All of the above"
        ]
      },
      {
        correct_answer: "All of the above",
        question: "Where can you buy cryptocurrency?",
        answers: [
          "A private transaction",
          "An exchange",
          "A Bitcoin ATM",
          "All of the above"
        ]
      },
      {
        correct_answer: "Mining",
        question: "Which is NOT a part of asymmetric encryption?",
        answers: [
          "Mining",
          "Public key",
          "Passphrase",
          "Private Key"
        ]
      },
      {
        correct_answer: "A distributed ledger on a peer to peer network",
        question: "What is a blockchain?",
        answers: [
          "A distributed ledger on a peer to peer network",
          "A type of cryptocurrency",
          "An exchange",
          "A centralized ledger"
        ]
      },
      {
        correct_answer: "A decentralized application",
        question: "What is a dApp?",
        answers: [
          "A type of Cryptocurrency",
          "A condiment",
          "A type of blockchain",
          "A decentralized application"
        ]
      },
      {
        correct_answer: "A fork",
        question: "What is the term for when a blockchain splits?",
        answers: [
          "A fork",
          "A merger",
          "A sidechain",
          "A division"
        ]
      },
      {
        correct_answer: "Prevents double spending",
        question: "What is the purpose of a nonce?",
        answers: [
          "Follows nouns",
          "A hash function",
          "Prevents double spending",
          "Sends information to the blockchain network"
        ]
      },
      {
        correct_answer: "A private key not connected to the Internet",
        question: "What is cold storage?",
        answers: [
          "A place to hang your coat",
          "A private key connected to the Internet",
          "A private key not connected to the Internet",
          "A desktop wallet"
        ]
      },
      {
        correct_answer: "A block reward",
        question: "What incentivizes the miners to give correct validation of transactions?",
        answers: [
          "A nonce",
          "A block reward",
          "Thumbs up from the community",
          "More memory"
        ]
      },
      {
        correct_answer: "The first block of a Blockchain",
        question: "What is a genesis block?",
        answers: [
          "The first block of a Blockchain",
          "A famous block that hardcoded a hash of the Book of Genesis onto the blockchain",
          "The first block after each block halving",
          "The 2nd transaction of a Blockchain"
        ]
      },
      {
        correct_answer: "A key NOT to be given to the public",
        question: "What is a private key?",
        answers: [
          "A key on your key chain",
          "A key given to the public",
          "A key NOT to be given to the public",
          "A key that opens a secret door"
        ]
      },
      {
        correct_answer: "Gas",
        question: "What powers the Ethereum Virtual Machine?",
        answers: [
          "Gas",
          "Ether",
          "Bitcoin",
          "Block Rewards"
        ]
      },
      {
        correct_answer: "Public and Private keys",
        question: "Asymmetric encryption uses:",
        answers: [
          "Public keys only",
          "Private keys only",
          "Public and Private keys",
          "Proof of Stake"
        ]
      },
      {
        correct_answer: "A transaction and block verification protocol",
        question: "What is Proof of Stake?",
        answers: [
          "A certificate needed to use the blockchain",
          "A password needed to access an exchange",
          "How private keys are made",
          "A transaction and block verification protocol"
        ]
      },
    ]
  )
}