import { ChainType, State } from "@/sections/near-intents/hooks/useConnectWallet";
import { useConnectedWalletsStore } from "@/stores/useConnectedWalletsStore";
import { useEffect, useMemo, useRef, useState } from "react";
import { useAccount } from "wagmi";
export default function useNearWallet() {
  const isNearPage = true
  const { address, } = useAccount();
  const [currentWallet, setCurrentWallet] = useState(null)
  const account = useMemo(() => {
    if (isNearPage && currentWallet) {
      if (currentWallet.chainType === ChainType.Near && currentWallet?.address && currentWallet.address.length < 30) {
        return currentWallet.address;
      }
      return currentWallet.address ? currentWallet.address : "";
    }
  }, [isNearPage, currentWallet]);
  useEffect(() => {
    const state = useConnectedWalletsStore.getState();
    setCurrentWallet(state.connectedWallets.length === 0 ? null : state.connectedWallets[0])
    const unsubscribe = useConnectedWalletsStore.subscribe((state) => {
      if (!state) return;
      setCurrentWallet(state.connectedWallets.length === 0 ? null : state.connectedWallets[0])
    });
    return () => {
      unsubscribe();
    };
  }, []);

  return {
    account
  }
}