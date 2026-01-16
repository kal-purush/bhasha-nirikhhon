import ganache, { Server } from "ganache";
import * as dotenv from "dotenv";
import { Wallet } from "ethers";
import { JsonRpcProvider } from "@ethersproject/providers";
import { contextParamsLocalChain } from "./constants";
import { Signer } from "@ethersproject/abstract-signer";

dotenv.config({ path: "env/.env" });

export class GanacheServer {
    public static instance: Server;
    public static initialAccounts: any[];
    public static CHAIN_ID = 24680;
    public static PORT = 7545;

    public static async start() {
        if (GanacheServer.initialAccounts === undefined) {
            GanacheServer.initialAccounts = GanacheServer.CreateInitialAccounts();
        }

        GanacheServer.instance = ganache.server({
            chain: {
                chainId: GanacheServer.CHAIN_ID
            },
            miner: {
                blockGasLimit: 80000000,
                defaultGasPrice: 800
            },
            logging: {
                quiet: true
            },
            wallet: {
                accounts: GanacheServer.initialAccounts
            }
        });
        await GanacheServer.instance.listen(GanacheServer.PORT);
        return GanacheServer.instance;
    }

    public static CreateInitialAccounts(): any[] {
        const accounts: string[] = [];
        const reg_bytes64: RegExp = /^(0x)[0-9a-f]{64}$/i;
        if (
            process.env.DEPLOYER !== undefined &&
            process.env.DEPLOYER.trim() !== "" &&
            reg_bytes64.test(process.env.DEPLOYER)
        ) {
            accounts.push(process.env.DEPLOYER);
        } else {
            process.env.DEPLOYER = Wallet.createRandom().privateKey;
            accounts.push(process.env.DEPLOYER);
        }

        if (process.env.OWNER !== undefined && process.env.OWNER.trim() !== "" && reg_bytes64.test(process.env.OWNER)) {
            accounts.push(process.env.OWNER);
        } else {
            process.env.OWNER = Wallet.createRandom().privateKey;
            accounts.push(process.env.OWNER);
        }

        if (
            process.env.VALIDATOR1 !== undefined &&
            process.env.VALIDATOR1.trim() !== "" &&
            reg_bytes64.test(process.env.VALIDATOR1)
        ) {
            accounts.push(process.env.VALIDATOR1);
        } else {
            process.env.VALIDATOR1 = Wallet.createRandom().privateKey;
            accounts.push(process.env.VALIDATOR1);
        }

        if (
            process.env.VALIDATOR2 !== undefined &&
            process.env.VALIDATOR2.trim() !== "" &&
            reg_bytes64.test(process.env.VALIDATOR2)
        ) {
            accounts.push(process.env.VALIDATOR2);
        } else {
            process.env.VALIDATOR2 = Wallet.createRandom().privateKey;
            accounts.push(process.env.VALIDATOR2);
        }

        if (
            process.env.VALIDATOR3 !== undefined &&
            process.env.VALIDATOR3.trim() !== "" &&
            reg_bytes64.test(process.env.VALIDATOR3)
        ) {
            accounts.push(process.env.VALIDATOR3);
        } else {
            process.env.VALIDATOR3 = Wallet.createRandom().privateKey;
            accounts.push(process.env.VALIDATOR3);
        }

        if (process.env.USER1 !== undefined && process.env.USER1.trim() !== "" && reg_bytes64.test(process.env.USER1)) {
            accounts.push(process.env.USER1);
        } else {
            process.env.USER1 = Wallet.createRandom().privateKey;
            accounts.push(process.env.USER1);
        }

        if (process.env.USER2 !== undefined && process.env.USER2.trim() !== "" && reg_bytes64.test(process.env.USER2)) {
            accounts.push(process.env.USER2);
        } else {
            process.env.USER2 = Wallet.createRandom().privateKey;
            accounts.push(process.env.USER2);
        }

        return accounts.map((m) => {
            return {
                balance: "0x100000000000000000000",
                secretKey: m
            };
        });
    }

    public static accounts(): Wallet[] {
        if (GanacheServer.initialAccounts === undefined) {
            GanacheServer.initialAccounts = GanacheServer.CreateInitialAccounts();
        }
        return GanacheServer.initialAccounts.map((m) =>
            new Wallet(m.secretKey).connect(GanacheServer.createTestProvider())
        );
    }

    public static createTestProvider(): JsonRpcProvider {
        return new JsonRpcProvider(`http://localhost:${GanacheServer.PORT}`, {
            chainId: GanacheServer.CHAIN_ID,
            name: "bosagora_devnet"
        });
    }

    public static setTestProvider(provider: JsonRpcProvider) {
        contextParamsLocalChain.web3Providers = provider;
    }

    public static setTestWeb3Signer(signer: Signer) {
        contextParamsLocalChain.signer = signer;
    }
}