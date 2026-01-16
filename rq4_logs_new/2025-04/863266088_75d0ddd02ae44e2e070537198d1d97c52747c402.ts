import { Contract } from 'ethers';
import { LPSTAKING_ABI } from './abi';
import { getEstimateGas } from '@/sections/vaults/v2/components/action/union/berapaw/utils';

export default async function onClaim(params: any) {
  const {
    currentRecord,
    signer,
    account,
  } = params;

  let contract: any;
  let method: string = "";
  let contractParams: any = [];

  contract = new Contract(
    currentRecord.vaultAddress,
    LPSTAKING_ABI,
    signer
  );
  method = "getAllRewards";
  contractParams = [account];

  const options = await getEstimateGas(contract, method, contractParams);
  return contract[method](...contractParams, options);
}