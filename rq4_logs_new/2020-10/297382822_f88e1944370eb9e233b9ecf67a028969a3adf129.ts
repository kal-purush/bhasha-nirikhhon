/**
 * This script is used to start a local private Ethereum testnet using Ganache-core
 */

import prompts from "prompts";
import Wallet from 'ethereumjs-wallet'
import BN from "bn.js";
import * as child_process from "child_process";

(async () => {
    let response0 = await prompts({
        type: "confirm",
        name: "generateAccount",
        message: "Do you want to use automatically generated accounts?",
        initial: true,
    });
    let accounts: { secretKey: string, balance: string }[] | undefined;
    if (response0.generateAccount === undefined) {
        return;
    } else if (!response0.generateAccount) {
        let response1 = await prompts({
            type: "text",
            name: "alloc",
            message: "What is your account ETH allocation config? format: JSON {[privateKey]: balance}",
            validate: input => {
                try {
                    const alloc = JSON.parse(input);
                    for (let privateKey of Object.keys(alloc)) {
                        const balance = alloc[privateKey];
                        try {
                            if (privateKey.startsWith("0x")) {
                                privateKey = privateKey.slice(2);
                            }
                            Wallet.fromPrivateKey(Buffer.from(privateKey, "hex"));
                        } catch (e) {
                            return `Invalid private key: ${e.toString()}`;
                        }
                    }
                }catch (e){
                    return `Invalid format, example: {"0xeee892cf591ae7d941e5b38bc98c900e430e1821046384bc756a3fe9e7840204":"1000000000000000000"}`
                }
                return true;
            }
        });
        const alloc = JSON.parse(response1.alloc);
        accounts = [];
        for (let privateKey of Object.keys(alloc)) {
            const b = alloc[privateKey];
            if (!privateKey.startsWith("0x")) {
                privateKey = "0x" + privateKey;
            }
            accounts.push({
                secretKey: privateKey,
                balance: "0x" + new BN(b).toString("hex")
            });
        }
    } else {
        accounts = undefined;
    }

    let response3 = await prompts({
        type: "text",
        name: "host",
        message: "What is the host name to listen on?",
        initial: "127.0.0.1",
    })

    const host = response3.host;

    let response2 = await prompts({
        type: "number",
        name: "port",
        message: "Which port do you want blockchain to run at?",
        initial: 8545,
        float: false,
        min: 0,
        max: 65535,
    });
    const port = response2.port;

    const args = [`--port`, port, `--host`, host,];
    if (accounts) {
        for (let alloc of accounts) {
            args.push("--account", `${alloc.secretKey},${alloc.balance}`);
        }
    }
    child_process.spawnSync("ganache-cli", args, {
        stdio: "inherit"
    });

})()