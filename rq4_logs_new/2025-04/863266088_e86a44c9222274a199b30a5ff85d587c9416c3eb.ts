import vaultAbi from "./abi";
import { multicall, multicallAddresses } from "@/utils/multicall";
import { DEFAULT_CHAIN_ID } from "@/configs";

export default async function getInfo({ provider, address }: any) {
  const multicallAddress = multicallAddresses[DEFAULT_CHAIN_ID];
  const result = await multicall({
    abi: vaultAbi,
    options: {},
    calls: [
      {
        address: address,
        name: "getCurrentEpochInfo",
        params: []
      },
      {
        address: address,
        name: "custodied",
        params: []
      }
    ],
    multicallAddress,
    provider
  });

  return {
    fundingStart: result[0][0].fundingStart.toNumber() * 1000,
    epochStart: result[0][0].epochStart.toNumber() * 1000,
    epochEnd: result[0][0].epochEnd.toNumber() * 1000,
    custodied: !!result[1]
  };
}