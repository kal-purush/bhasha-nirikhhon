import {
  type Address,
  useAccount,
  usePrepareContractWrite,
  useContractWrite,
  useWaitForTransaction,
} from "wagmi";
import { RELIC_CONTRACT, RELIC_ABI } from "~/utils/abi/relicAbi";

// example here: https://wagmi.sh/examples/contract-write

export function useTransferRelic(toAddress: Address, relicId: number) {
  const account = useAccount();
  const { config, isError: isPrepareError } = usePrepareContractWrite({
    address: RELIC_CONTRACT,
    abi: RELIC_ABI,
    functionName: "safeTransferFrom",
    args: [account.address, toAddress, relicId],
    enabled: relicId > 0,
  });
  const { data, isError, write } = useContractWrite(config);
  const { isLoading, isSuccess } = useWaitForTransaction({ hash: data?.hash });

  return {
    transfer: write,
    isLoading,
    isSuccess,
    isPrepareError,
    isError,
  };
}