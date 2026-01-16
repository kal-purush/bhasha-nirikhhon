import {
  ArgentDapp,
  ArgentDappMap,
  ArgentUserDapp,
  ArgentToken,
  ArgentTokenValue,
  ArgentTokenMap,
  ArgentUserToken,
} from "types/backTypes";

const API_BASE = "cloud.argent-api.com";
const API_VERSION = "v1";

export const fetchDapps = async () => {
  try {
    const response = await fetch(
      `https://${API_BASE}/${API_VERSION}/tokens/dapps?chain=starknet`
    );
    const data: ArgentDapp[] = await response.json();

    return Object.fromEntries(
      data.map((dapp) => [dapp.dappId, dapp])
    ) as ArgentDappMap;
  } catch (err) {
    console.log("Error while fetching dapps from Argent API", err);
    throw new Error("Error while fetching dapps from Argent API");
  }
};

export const fetchTokens = async () => {
  try {
    const response = await fetch(
      `https://${API_BASE}/${API_VERSION}/tokens/info?chain=starknet`
    );
    const data: { tokens: [ArgentToken] } = await response.json();

    return Object.fromEntries(
      data.tokens.map((token) => [token.address, token])
    ) as ArgentTokenMap;
  } catch (err) {
    console.log("Error while fetching token from Argent API", err);
    throw new Error("Error while fetching token from Argent API");
  }
};

export const fetchUserTokens = async (walletAddress: string) => {
  const opts = {
    headers: {
      "argent-client": "portfolio",
      "argent-network": "mainnet",
      "argent-version": "1.4.3",
    },
  };

  try {
    const response = await fetch(
      `https://${API_BASE}/${API_VERSION}/activity/starknet/mainnet/account/${walletAddress}/balance`,
      opts
    );
    const data: { balances: ArgentUserToken[]; status: string } =
      await response.json();
    return data.balances;
  } catch (err) {
    console.log("Error while fetching wallet dapps from Argent API", err);
    throw new Error("Error while fetching wallet dapps from Argent API");
  }
};

export const fetchUserDapps = async (walletAddress: string) => {
  try {
    const response = await fetch(
      `https://${API_BASE}/${API_VERSION}/tokens/defi/decomposition/${walletAddress}?chain=starknet`
    );
    const data: { dapps: ArgentUserDapp[] } = await response.json();
    return data.dapps;
  } catch (err) {
    console.log("Error while fetching wallet dapps from Argent API", err);
    throw new Error("Error while fetching wallet dapps from Argent API");
  }
};

export const calculateTokenPrice = async (
  tokenAddress: string,
  tokenAmount: Big.Big,
  currency: "USD" | "EUR" | "GBP" = "USD"
) => {
  try {
    const response = await fetch(
      `https://${API_BASE}/${API_VERSION}/tokens/prices/${tokenAddress}?chain=starknet&currency=${currency}`
    );
    const data: ArgentTokenValue = await response.json();
    return tokenAmount.mul(data.ccyValue).toNumber();
  } catch (err) {
    console.log("Error while fetching token price from Argent API", err);
    throw new Error("Error while fetching token price from Argent API");
  }
};