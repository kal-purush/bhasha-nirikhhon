// import { createConfig } from "wagmi";
// import { foundry } from "viem/chains";
// import { http } from "viem";
// import { injected } from "wagmi/connectors";

// // Create wagmi config for Anvil (Foundry)
// export const wagmiConfig = createConfig({
//   //   autoConnect: true,
//   connectors: [
//     injected(), // injected connector for MetaMask or other injected wallets
//   ],
//   chains: [foundry],
//   transports: {
//     [foundry.id]: http(),
//   },
// });

// wagmiConfig.ts
import { http, createConfig } from "wagmi";
import { hardhat } from "wagmi/chains";
import { injected } from "wagmi/connectors";

export const wagmiConfig = createConfig({
  connectors: [
    injected(), // MetaMask or local wallet
  ],
  chains: [
    {
      ...hardhat,
      rpcUrls: {
        default: {
          http: ["http://127.0.0.1:8545"], // <- Anvil RPC
        },
      },
    },
  ],
  transports: {
    [hardhat.id]: http("http://127.0.0.1:8545"),
  },
});