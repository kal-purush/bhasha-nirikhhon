export const STELLAR_DECIMALS = 7;
export const SECONDS_PER_DAY = 86400;
export function getEmissionsPerDayPerUnit(eps: bigint, totalAmount: number, decimals?: number) {
  if (eps === BigInt(0) || totalAmount === 0) {
    return 0;
  }
  const epsNum = Number(Number(eps) / Math.pow(10, decimals || STELLAR_DECIMALS));
  const toReturn = Number((epsNum * SECONDS_PER_DAY) / totalAmount);
  console.log({ toReturn, eps, epsNum, totalAmount, decimals });
  const decimalCount = toReturn.toString().split('.')[1]?.length || 0;
  if (decimalCount > (decimals || STELLAR_DECIMALS)) {
    return Number(toReturn.toFixed(decimals));
  }
  return toReturn;
}