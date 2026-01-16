import { Alfajores, Mainnet, Network } from '@celo-tools/use-contractkit'

export { Alfajores, Mainnet } from '@celo-tools/use-contractkit'

export const Ethereum: Network = {
  name: 'Ethereum',
  chainId: 1,
  rpcUrl: `https://mainnet.infura.io/v3/${process.env.REACT_APP_INFURA_KEY}`,
  explorer: 'https://etherscan.io/',
  graphQl: '',
}

export const Polygon: Network = {
  name: 'Polygon',
  chainId: 137,
  rpcUrl: `https://polygon-mainnet.infura.io/v3/${process.env.REACT_APP_INFURA_KEY}`,
  explorer: 'https://polygonscan.com/',
  graphQl: '',
}

export const MainnetNetworks = [Mainnet, Ethereum, Polygon]

export const Kovan: Network = {
  name: 'Kovan',
  chainId: 42,
  rpcUrl: `https://kovan.infura.io/v3/${process.env.REACT_APP_INFURA_KEY}`,
  explorer: 'https://kovan.etherscan.io/',
  graphQl: '',
}

export const Rinkeby: Network = {
  name: 'Rinkeby',
  chainId: 4,
  rpcUrl: `https://rinkeby.infura.io/v3/${process.env.REACT_APP_INFURA_KEY}`,
  explorer: 'https://rinkeby.etherscan.io/',
  graphQl: '',
}

export const DevNetworks = [Alfajores, Kovan, Rinkeby]