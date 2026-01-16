import { TransactionEnvelope } from "@saberhq/solana-contrib";
import { setupEnv } from "./helpers";
import { transfer_token_authority } from "./transfer_token_authority";
import {
    getATAAddress,
    getOrCreateATA,
    TOKEN_PROGRAM_ID,
    createMintInstructions,
} from "@saberhq/token-utils";
import {
    Keypair,
    PublicKey,
    SystemProgram,
    Transaction,
} from "@solana/web3.js";
import * as fs from 'fs';
import { chaiSolana, expectTX } from "@saberhq/chai-solana";
import chai, { expect } from "chai";
chai.use(chaiSolana);


var create_new_token = async () => {
    const { provider } = setupEnv();

    const decodedKey = new Uint8Array(
        JSON.parse(
            //replace with actual path from home dir. For example '.config/solana/devnet.json'
            fs.readFileSync('./cli/met.json', 'utf8')
        ));
    let mint = Keypair.fromSecretKey(decodedKey);
    console.log("create new token address: ", mint.publicKey.toString())

    const instructions = await createMintInstructions(
        provider,
        provider.wallet.publicKey,
        mint.publicKey,
    );

    const tx = new Transaction();
    tx.add(...instructions);

    try {
        let pendingTx = await provider.send(tx, [mint]);
        console.log("signature: ", pendingTx.signature);
    } catch (err) {
        console.log(err);
    }


    // transfer authority to phantom in order to create reaml DAO 
    await transfer_token_authority();

}

create_new_token()