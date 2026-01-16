import { BalancerLib } from "../libs/balancer.lib";
import axios from "axios";

type QuoterProps = {
  inputCurrency: any;
  outputCurrency: any;
  inputAmount: string;
  slippage: number;
  account: string;
};

export class Burrbear {
  private balancer: any;
  private ROUTER_ADDRESS: { [key: number]: string } = {
    80094: "0xBE09E71BDc7b8a50A05F7291920590505e3C7744"
  };

  constructor(chainId: number) {
    this.balancer = new BalancerLib({
      routerAddress: this.ROUTER_ADDRESS[chainId]
    });
  }
  public async quoter(params: QuoterProps): Promise<any> {
    const poolsResponse = await axios.post(
      "https://api.goldsky.com/api/public/project_cluukfpdrw61a01xag6yihcuy/subgraphs/berachain/prod/gn",
      {
        query:
          "query AllPools($skip: Int, $first: Int, $orderBy: Pool_orderBy, $orderDirection: OrderDirection, $where: Pool_filter, $block: Block_height) {\n  pool0: pools(\n    first: 1000\n    orderBy: $orderBy\n    orderDirection: $orderDirection\n    where: $where\n    block: $block\n  ) {\n    ...SubgraphPool\n  }\n  pool1000: pools(\n    first: 1000\n    skip: 1000\n    orderBy: $orderBy\n    orderDirection: $orderDirection\n    where: $where\n    block: $block\n  ) {\n    ...SubgraphPool\n  }\n  pool2000: pools(\n    first: 1000\n    skip: 2000\n    orderBy: $orderBy\n    orderDirection: $orderDirection\n    where: $where\n    block: $block\n  ) {\n    ...SubgraphPool\n  }\n}\n\nfragment SubgraphPool on Pool {\n  id\n  address\n  poolType\n  poolTypeVersion\n  factory\n  strategyType\n  symbol\n  name\n  swapEnabled\n  swapFee\n  protocolYieldFeeCache\n  protocolSwapFeeCache\n  owner\n  totalWeight\n  totalSwapVolume\n  totalSwapFee\n  totalLiquidity\n  totalShares\n  tokens(first: 100, orderBy: index) {\n    ...SubgraphPoolToken\n  }\n  swapsCount\n  holdersCount\n  tokensList\n  amp\n  priceRateProviders(first: 100) {\n    ...SubgraphPriceRateProvider\n  }\n  expiryTime\n  unitSeconds\n  createTime\n  principalToken\n  baseToken\n  wrappedIndex\n  mainIndex\n  lowerTarget\n  upperTarget\n  sqrtAlpha\n  sqrtBeta\n  root3Alpha\n  isInRecoveryMode\n  isPaused\n  alpha\n  beta\n  c\n  s\n  lambda\n  tauAlphaX\n  tauAlphaY\n  tauBetaX\n  tauBetaY\n  u\n  v\n  w\n  z\n  dSq\n  delta\n  epsilon\n  quoteToken\n}\n\nfragment SubgraphPoolToken on PoolToken {\n  id\n  symbol\n  name\n  decimals\n  address\n  balance\n  managedBalance\n  weight\n  priceRate\n  isExemptFromYieldProtocolFee\n  token {\n    ...TokenTree\n  }\n}\n\nfragment TokenTree on Token {\n  latestUSDPrice\n  latestFXPrice\n  pool {\n    ...SubgraphSubPool\n    tokens(first: 100, orderBy: index) {\n      ...SubgraphSubPoolToken\n      token {\n        latestUSDPrice\n        pool {\n          ...SubgraphSubPool\n          tokens(first: 100, orderBy: index) {\n            ...SubgraphSubPoolToken\n            token {\n              latestUSDPrice\n              pool {\n                ...SubgraphSubPool\n              }\n            }\n          }\n        }\n      }\n    }\n  }\n}\n\nfragment SubgraphSubPool on Pool {\n  id\n  totalShares\n  address\n  poolType\n  mainIndex\n}\n\nfragment SubgraphSubPoolToken on PoolToken {\n  address\n  balance\n  weight\n  priceRate\n  symbol\n  decimals\n  isExemptFromYieldProtocolFee\n}\n\nfragment SubgraphPriceRateProvider on PriceRateProvider {\n  address\n  token {\n    address\n  }\n}\n",
        variables: {
          where: {
            swapEnabled: true,
            totalShares_gt: 0.00001,
            id_not_in: [
              "0x0b3ce9a5d7403f9a61a396d9fdd8f178ae734a12000200000000000000000001",
              "0x04c34cbe8dbe95188bce65808e5ba1b44a80b315000200000000000000000002",
              "0xf9154f85947e09efec7aefe0de55bbff796854d7000000000000000000000003",
              "0xebff6ef6a2f53b2ec29f47f9ccedcb5e6e7978cb000000000000000000000005",
              "0x412fd1939df8716b616b603ee582074c1e40bb6d000200000000000000000007"
            ]
          }
        },
        operationName: "AllPools"
      }
    );

    if (!poolsResponse.data.data?.pool0?.length) {
      return {
        outputCurrencyAmount: "",
        noPair: true
      };
    }
    const pools = poolsResponse.data.data.pool0.map((pool: any) => [
      pool.tokensList,
      pool.id
    ]);

    this.balancer.setPools(pools);
    const bestTrade = await this.balancer.bestTrade(params);

    return bestTrade;
  }
}