export const BERAPAW_MINT_ABI = [
  {
    inputs: [
      {
        internalType: 'address',
        name: 'user',
        type: 'address'
      },
      {
        internalType: 'address',
        name: 'rewardVault',
        type: 'address'
      },
      {
        internalType: 'address',
        name: 'recipient',
        type: 'address'
      }
    ],
    name: 'mint',
    outputs: [
      {
        internalType: 'uint256',
        name: '',
        type: 'uint256'
      }
    ],
    stateMutability: 'nonpayable',
    type: 'function'
  },
  {
    inputs: [
      {
        internalType: 'address',
        name: 'rewardVault',
        type: 'address'
      }
    ],
    name: 'bribeBack',
    outputs: [
      {
        internalType: 'address',
        name: 'recipient',
        type: 'address'
      },
      {
        internalType: 'uint96',
        name: 'percentual',
        type: 'uint96'
      }
    ],
    stateMutability: 'view',
    type: 'function'
  }
];

export const BERAPAW_APPROVE_ABI = [
  {
    inputs: [
      {
        internalType: 'address',
        name: '_operator',
        type: 'address'
      }
    ],
    name: 'setOperator',
    outputs: [
      {
        internalType: 'uint256',
        name: '',
        type: 'uint256'
      }
    ],
    stateMutability: 'nonpayable',
    type: 'function'
  },
  {
    inputs: [
      {
        internalType: 'address',
        name: '_operator',
        type: 'address'
      }
    ],
    name: 'operator',
    outputs: [
      {
        internalType: 'address',
        name: '',
        type: 'address'
      }
    ],
    stateMutability: 'nonpayable',
    type: 'function'
  }
];

export const BERAPAW_VAULT_ABI = [
  {
    type: "function",
    name: "earned",
    inputs: [
      {
        name: "account",
        type: "address",
        internalType: "address"
      }
    ],
    outputs: [
      {
        name: "",
        type: "uint256",
        internalType: "uint256"
      }
    ],
    stateMutability: "view"
  }
];