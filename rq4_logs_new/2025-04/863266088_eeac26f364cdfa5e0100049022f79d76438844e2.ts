export const STAKE_ABI = [
    {
      inputs: [
        {
          internalType: "address",
          name: "_token",
          type: "address"
        },
        {
          internalType: "uint256",
          name: "_amount",
          type: "uint256"
        }
      ],
      name: "mint",
      outputs: [],
      stateMutability: "nonpayable",
      type: "function"
    },
    {
      "inputs": [],
      "name": "totalSupply",
      "outputs": [
        {
          "internalType": "uint256",
          "name": "result",
          "type": "uint256"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    },
    {
      inputs: [
        {
          internalType: "address",
          name: "depositAsset",
          type: "address"
        },
        {
          internalType: "uint256",
          name: "depositAmount",
          type: "uint256"
        },
        {
          internalType: "uint256",
          name: "minimumMint",
          type: "uint256"
        }
      ],
      name: "deposit",
      outputs: [],
      stateMutability: "nonpayable",
      type: "function"
    },
    {
      inputs: [
        { internalType: 'address', name: 'account', type: 'address' }
      ],
      name: 'balanceOf',
      outputs: [
        { internalType: 'uint256', name: '', type: 'uint256' }
      ],
      stateMutability: 'view',
      type: 'function'
    }
  ];