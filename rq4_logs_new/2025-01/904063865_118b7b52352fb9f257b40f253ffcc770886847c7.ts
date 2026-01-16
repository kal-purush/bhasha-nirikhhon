import { Chain } from "@rainbow-me/rainbowkit";
import { baseSepolia, hardhat, mainnet } from "viem/chains";

export const defaultChain =
  process.env.NODE_ENV === "production" ? "baseSepolia" : "hardhat";

export const chains = Object.entries({
  // base,
  // optimism,
  hardhat,
  baseSepolia,
  mainnet,
}).reduce(
  (acc, [key, chain]) => {
    if (process.env.NODE_ENV === "production" && chain.id === hardhat.id)
      return acc;
    return { ...acc, [key]: chain };
  },
  {} as Record<string, Chain>
);