import { SigningStargateClient, coins } from "@cosmjs/stargate";
import { SignArbSecp256k1HdWallet } from "./SignArbSecp256k1HdWallet"; // Adjust path
import { MsgGrantAllowance } from "cosmjs-types/cosmos/feegrant/v1beta1/tx";
import { BasicAllowance } from "cosmjs-types/cosmos/feegrant/v1beta1/feegrant";
import { Any } from "cosmjs-types/google/protobuf/any";
import { Registry } from "@cosmjs/proto-signing";

async function setupFeeGrant() {
  // Granter's mnemonic (replace with the actual mnemonic for xion1yg2fmythclttenp5hwh6fwhre7y9crful2c9lp)
  const granterMnemonic = "mesh mother student depart pudding credit year couch gown festival volcano update pumpkin picnic girl pull cram future noise salmon unable ceiling flock remain"; // e.g., "word1 word2 ..."
  const granterWallet = await SignArbSecp256k1HdWallet.fromMnemonic(granterMnemonic, {
    prefix: "xion",
  });

  // Create a custom registry with fee grant types
  const registry = new Registry();
  registry.register("/cosmos.feegrant.v1beta1.MsgGrantAllowance", MsgGrantAllowance);
  registry.register("/cosmos.feegrant.v1beta1.BasicAllowance", BasicAllowance);

  // Connect to Xion testnet with the custom registry
  const rpcEndpoint = "https://rpc.xion-testnet-1.burnt.com:443";
  const client = await SigningStargateClient.connectWithSigner(rpcEndpoint, granterWallet, {
    registry,
  });

  // Get granter address
  const [granterAccount] = await granterWallet.getAccounts();
  const granterAddress = granterAccount.address;
  console.log("Granter Address:", granterAddress);
  const granteeAddress = "xion1r32yed7q9gj8c4mhpxhtzy9v732hp2e3ldz6z6";

  // Define the fee grant allowance (e.g., 1,000,000 uxion)
  const allowance = {
    typeUrl: "/cosmos.feegrant.v1beta1.BasicAllowance",
    value: BasicAllowance.fromPartial({
      spendLimit: coins(1000000, "uxion"), // Adjust amount as needed
      // expiration: null, // No expiration; set a Date object if desired (e.g., new Date("2026-01-01"))
    }),
  };

  // Encode allowance as Any
  const packedAllowance = Any.fromPartial({
    typeUrl: allowance.typeUrl,
    value: BasicAllowance.encode(allowance.value).finish(),
  });

  // Create the MsgGrantAllowance message
  const msg = {
    typeUrl: "/cosmos.feegrant.v1beta1.MsgGrantAllowance",
    value: MsgGrantAllowance.fromPartial({
      granter: granterAddress,
      grantee: granteeAddress,
      allowance: packedAllowance,
    }),
  };

  // Sign and broadcast the transaction
  const fee = {
    amount: coins(5000, "uxion"),
    gas: "200000",
  };

  try {
    const result = await client.signAndBroadcast(granterAddress, [msg], fee, "Granting fee allowance");
    console.log("Fee grant successful!");
    console.log("Transaction Hash:", result.transactionHash);
  } catch (error) {
    console.error("Error granting fee:", error);
  }
}

setupFeeGrant().then(() => process.exit(0)).catch((err) => {
  console.error(err);
  process.exit(1);
});