/**
 *  1 ETH = (10^18) wei; 1 ETH = $5,000 USD
 */

const WEI_IN_ETHER = BigInt(10 ** 18);
const ETH_TO_USD = 5000;

/**
 *
 * @param hexadecimal
 * @returns wei
 */
const hexToWei = (hexadecimal: string): number =>
  hexadecimal.length ? parseInt(hexadecimal, 16) : 0;

/**
 *
 * @param Wei
 * @returns ETH
 */
const weiToETH = (Wei: number): number => Number(Wei) / Number(WEI_IN_ETHER);

/**
 *
 * @param ETH
 * @returns USD
 */
const ethToUSD = (ETH: number): number => ETH * ETH_TO_USD;

/**
 *
 * @param Wei
 * @returns USD
 */

const weiToUSD = (Wei: number): number => {
  const eth = weiToETH(Wei);
  return ethToUSD(eth);
};

export { hexToWei, weiToUSD };