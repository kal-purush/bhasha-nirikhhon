import { Chain } from "wagmi";

export const GTN: Chain = {
  id: 101010,
  name: "General Trust Network",
  network: "general-trust-network",
  nativeCurrency: {
    decimals: 18,
    name: "Decentralized Native Token",
    symbol: "DNT",
  },
  testnet: true,
  blockExplorers: {
    default: {
      name: "General Trust Network",
      url: "https://explorer.stble.io/",
    },
  },
  rpcUrls: {
    default: {
      http: ["https://gtn.stabilityprotocol.com"],
    },
    public: {
      http: ["https://gtn.stabilityprotocol.com"],
    },
  },
  contracts: {
    multicall3: {
      // https://raw.githubusercontent.com/mds1/multicall/main/src/Multicall3.sol
      address: "0xBA2923DAe45aD6b8B77bff4733c75b0C13F0ce2d",
      blockCreated: 453,
    },
  },
};