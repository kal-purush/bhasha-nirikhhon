import { atom, useAtom } from "jotai";
import { atomWithStorage } from "jotai/utils";
import { PassKeyKeyPair } from "../passkey/WebAuthnWrapper";


export const walletAddressAtom = atomWithStorage(
  "0xequity/v2/walletAddress",
  "",
);

const mnemonicPhraseAtom = atomWithStorage<{
  [x: string]: string;
}>("0xequity/v2/mnemonicPhrase", {});

const setWalletAddressAtom = atom<undefined, string[], void>(
  undefined,
  (_get, set, update) => set(walletAddressAtom, update),
);

const setMnemonicPhraseAtom = atom<undefined, any[], void>(
  undefined,
  (_get, set, update) => set(mnemonicPhraseAtom, update),
);

const walletWeb3AddressAtom = atomWithStorage(
  "0xequity/v2/web3/walletAddress",
  "",
);

const setWalletWeb3AddressAtom = atom<undefined, string[], void>(
  undefined,
  (_get, set, update) => set(walletWeb3AddressAtom, update),
);

const walletWeb3TypeAtom = atomWithStorage("0xequity/v2/web3/walletType", "");

const setWalletWeb3TypeAtom = atom<undefined, string[], void>(
  undefined,
  (_get, set, update) => set(walletWeb3TypeAtom, update),
);
// 0 -> Choose
// 1 -> PassKey
// 2 -> Web3
// 3 -> Passkey + web3
const walletTypeAtom = atomWithStorage("0xequity/v2/walletType", 0);

const setWalletTypeAtom = atom<undefined, number[], void>(
  undefined,
  (_get, set, update) => set(walletTypeAtom, update),
);

const isConnectedAtom = atomWithStorage("0xequity/v2/isConnected", false);

const setIsConnectedAtom = atom<undefined, boolean[], void>(
  undefined,
  (_get, set, update) => set(isConnectedAtom, update),
);

const walletBalanceAtom = atom("");

const setWalletBalanceAtom = atom<undefined, string[], void>(
  undefined,
  (_get, set, update) => set(walletBalanceAtom, update),
);

const walletCrendentialIdAtom = atom("");

const setWalletCrendentialIdAtom = atom<undefined, string[], void>(
  undefined,
  (_get, set, update) => set(walletCrendentialIdAtom, update),
);

const walletDecodedKeysAtom = atom([]);

const setWalletDecodedKeysAtom = atom<undefined, any, void>(
  undefined,
  (_get, set, update) => set(walletDecodedKeysAtom, update),
);

const setDisconnectAtom = atom<undefined, any[], void>(
  undefined,
  (_get, set, _) => {
    set(walletAddressAtom, "");
    set(walletTypeAtom, 0);
    set(walletWeb3TypeAtom, "");
    set(isConnectedAtom, false);
  },
);

interface ISetWeb3WalletLocal {
  mnemonic: {
    [x: string]: string;
  };
  walletType: number;
  walletWeb3Type: string;
  walletAddress: string;
  web3WalletAddress: string;
}
const setWeb3LocalWalletAtom = atom<undefined, ISetWeb3WalletLocal[], void>(
  undefined,
  (_get, set, update) => {
    set(setMnemonicPhraseAtom, update.mnemonic);
    set(setWalletTypeAtom, update.walletType);
    set(setWalletWeb3TypeAtom, update.walletWeb3Type);
    set(setWalletWeb3AddressAtom, update.web3WalletAddress);
    set(setWalletAddressAtom, update.walletAddress);
  },
);

const passKeysAtom = atomWithStorage<PassKeyKeyPair[]>(
  "0xequity/v2/passKeys",
  [],
);

const setPassKeysAtom = atom<undefined, PassKeyKeyPair[][], void>(
  undefined,
  (_get, set, update) => set(passKeysAtom, update),
);

export const useWalletJotai = () => {
  const [walletAddress] = useAtom(walletAddressAtom);
  const [, setWalletAddress] = useAtom(setWalletAddressAtom);

  const [walletType] = useAtom(walletTypeAtom);
  const [, setWalletType] = useAtom(setWalletTypeAtom);

  const [walletWeb3Type] = useAtom(walletWeb3TypeAtom);
  const [, setWalletWeb3Type] = useAtom(setWalletWeb3TypeAtom);

  const [walletWeb3Address] = useAtom(walletWeb3AddressAtom);
  const [, setWalletWeb3Address] = useAtom(setWalletWeb3AddressAtom);

  const [walletBalance] = useAtom(walletBalanceAtom);
  const [, setWalletBalance] = useAtom(setWalletBalanceAtom);

  const [, setDisconnect] = useAtom(setDisconnectAtom);

  const [isConnected] = useAtom(isConnectedAtom);
  const [, setIsConnected] = useAtom(setIsConnectedAtom);

  const [mnemonicPhrase] = useAtom(mnemonicPhraseAtom);
  const [, setMnemonicPhrase] = useAtom(setMnemonicPhraseAtom);
  const [, setWeb3LocalWallet] = useAtom(setWeb3LocalWalletAtom);

  const [walletDecodedKeys] = useAtom(walletDecodedKeysAtom);
  const [, setWalletDecodedKeys] = useAtom(setWalletDecodedKeysAtom);

  const [passKeys] = useAtom(passKeysAtom);
  const [, setPassKeys] = useAtom(setPassKeysAtom);

  const [walletCrendentialId] = useAtom(walletCrendentialIdAtom);
  const [, setWalletCrendentialId] = useAtom(setWalletCrendentialIdAtom);

  return {
    setDisconnect,
    walletType,
    walletCrendentialId,
    setWalletCrendentialId,
    setWeb3LocalWallet,
    setWalletType,
    setWalletDecodedKeys,
    walletDecodedKeys,
    walletWeb3Address,
    setWalletWeb3Address,
    walletWeb3Type,
    setWalletWeb3Type,
    walletBalance,
    setWalletBalance,
    walletAddress,
    setWalletAddress,
    isConnected,
    setIsConnected,
    passKeys,
    setPassKeys,
    mnemonicPhrase,
    setMnemonicPhrase,
  };
};