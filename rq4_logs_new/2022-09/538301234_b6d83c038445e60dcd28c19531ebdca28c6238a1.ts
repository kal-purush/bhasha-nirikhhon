import { ChainId } from "@pancakeswap/sdk";
import { ERC20_ABI } from "config/abi/erc20";
import tokens from "config/constants/tokens";
import { useEffect, useState } from "react";
import multicall from "utils/multicall";

export const GetBalance = (walletAddress, totalLimit) => {
  const [balanceList, setBalanceList] = useState([{
    name: '',
    balance: 0,
    limit: 0
  }]);

  console.log('address',totalLimit);
  
  useEffect(() => {
    const getBalanceList = async () => {
      try {
        const listWallet = []

        for (let index = 0; index < totalLimit.length; index++) {
          listWallet.push({
            address: totalLimit[index].address,
            name: 'balanceOf',
            params: [walletAddress]
          })

        }
        const resultListBlock = await multicall(ERC20_ABI, listWallet)
        const result = resultListBlock.map((item, key) => {
          return {
            name: totalLimit[key].name,
            balance: Number((resultListBlock[key]?.toString())) / 1E18,
            limit: totalLimit[key].limit
          }
        })
        setBalanceList(result)
      }
      catch (error) {
        console.log(error)
        setBalanceList([{
          name: '',
          balance: 0,
          limit: 0
        }])
      }
    }
    if (totalLimit.length > 0) {
      getBalanceList()
    } else {
      setBalanceList([{
        name: '',
        balance: 0,
        limit: 0
      }])
    }
  }, [walletAddress, totalLimit.length, totalLimit])
  return { balanceList }
}