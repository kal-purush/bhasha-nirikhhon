import './App.css';
import Navbar from './components/Navbar_module/Navbar';
import { Route, BrowserRouter as Router, Routes } from "react-router-dom";
import Home from './pages/Home/Home';
import "@rainbow-me/rainbowkit/dist/index.css";
import { getDefaultWallets, RainbowKitProvider } from "@rainbow-me/rainbowkit";
import CloseIcon from '@mui/icons-material/Close';
import { alchemyProvider } from "wagmi/providers/alchemy";
import { publicProvider } from "wagmi/providers/public";
import { darkTheme } from "@rainbow-me/rainbowkit";
import { chain, configureChains, createClient, WagmiConfig } from "wagmi";


function App() {
  const BSCTestnetChain = {
    id: 97,
    name: "BSC test",
    network: "BSC test",
    iconUrl: "https://www.logo.wine/a/logo/Binance/Binance-Icon-Logo.wine.svg",
    iconBackground: "#fff",
    nativeCurrency: {
      decimals: 18,
      name: "Binance Smart Chain",
      symbol: "BNB",
    },
    rpcUrls: {
      default: "https://bsctestapi.terminet.io/rpc",
    },
    blockExplorers: {
      default: { name: "SnowTrace", url: "https://bscscan.com" },
      etherscan: { name: "SnowTrace", url: "https://bscscan.com" },
    },
    testnet: true,
  };

  const Avaxchain = {
    id: 43114,
    name: "Avalanche Network",
    network: "Avalanche Network",
    iconUrl: "https://cryptologos.cc/logos/avalanche-avax-logo.png",
    iconBackground: "#fff",
    nativeCurrency: {
      decimals: 18,
      name: "Avalanche",
      symbol: "AVAX",
    },
    rpcUrls: {
      default: "https://rpc.ankr.com/avalanche",
    },
    blockExplorers: {
      default: { name: "SnowTrace", url: "https://snowtrace.io/" },
      etherscan: { name: "SnowTrace", url: "https://snowtrace.io/" },
    },
    testnet: false,
  };

  const { chains, provider } = configureChains(
    [Avaxchain, BSCTestnetChain, chain.mainnet, chain.polygon, chain.optimism, chain.arbitrum],
    [alchemyProvider({ alchemyId: process.env.ALCHEMY_ID }), publicProvider()]
  );

  const { connectors } = getDefaultWallets({
    appName: "baby-cracken",
    chains,
  });
  const wagmiClient = createClient({
    autoConnect: true,
    connectors,
    provider,
  });
  
  return (
    <>
          <WagmiConfig client={wagmiClient}>
            <RainbowKitProvider
              chains={chains}
              theme={darkTheme({ borderRadius: "medium" })}
            >
          <Router>
            <div>
              <Routes>
                <Route exact path="/" element={<Home />} />
              </Routes>
            </div>
          </Router>
          </RainbowKitProvider>
          </WagmiConfig>
    </>
  );
}

export default App;