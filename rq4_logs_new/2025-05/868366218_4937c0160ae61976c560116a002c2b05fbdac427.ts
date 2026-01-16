import {
  useReadContract,
  useWriteContract,
  useWaitForTransactionReceipt,
  useAccount,
} from "wagmi";
import { Abi } from "viem";
import CONTRACT_ADDRESS from "../contract-address";
import FACTORY_ABI from "../abi/factory-contract-abi.json";
import { useEffect, useState } from "react";

const factoryContractAddress =
  CONTRACT_ADDRESS.factoryContractAddress as `0x${string}`;

export const useGetUserCollectionController = (userAddress: string) => {
  const {
    data: collections,
    isLoading,
    isError,
  } = useReadContract({
    address: factoryContractAddress,
    abi: FACTORY_ABI.abi as Abi,
    functionName: "getUserCollections",
    args: [userAddress],
  });

  return { collections, isLoading, isError };
};