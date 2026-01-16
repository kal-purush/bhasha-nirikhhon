export default [
  {
    inputs: [
      { internalType: "uint256", name: "pid", type: "uint256" },
      { internalType: "uint256", name: "amount", type: "uint256" },
      { internalType: "address", name: "to", type: "address" }
    ],
    name: "deposit",
    outputs: [],
    stateMutability: "nonpayable",
    type: "function"
  },
  {
    inputs: [
      { internalType: "uint256", name: "pid", type: "uint256" },
      { internalType: "uint256", name: "amount", type: "uint256" },
      { internalType: "address", name: "to", type: "address" }
    ],
    name: "withdraw",
    outputs: [],
    stateMutability: "nonpayable",
    type: "function"
  },
  {
    inputs: [
      { internalType: "uint256", name: "pid", type: "uint256" },
      { internalType: "address", name: "to", type: "address" }
    ],
    name: "harvest",
    outputs: [],
    stateMutability: "nonpayable",
    type: "function"
  }
];