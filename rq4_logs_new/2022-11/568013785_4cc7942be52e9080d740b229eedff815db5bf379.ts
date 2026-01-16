import '@rainbow-me/rainbowkit/styles.css';
import {
    getDefaultWallets,
} from '@rainbow-me/rainbowkit';
import {
    configureChains,
    createClient,
    Chain
} from 'wagmi';

import { publicProvider } from 'wagmi/providers/public';
const tFil: Chain = {
    id: 31415,
    name: "Filecoin — Wallaby testnet",
    network: "tfil",
    rpcUrls: {
        default: "https://wallaby.node.glif.io/rpc/v0"
    },
    nativeCurrency: {
        name: "FIL",
        decimals: 18,
        symbol: "tFIL"
    },
    testnet: true
}


const { chains, provider } = configureChains(
    [tFil],
    [
        publicProvider()
    ]
);

const { connectors } = getDefaultWallets({
    appName: 'DFP',
    chains
});

export const Chains = chains
export const wagmiClient = createClient({
    autoConnect: true,
    connectors,
    provider
})