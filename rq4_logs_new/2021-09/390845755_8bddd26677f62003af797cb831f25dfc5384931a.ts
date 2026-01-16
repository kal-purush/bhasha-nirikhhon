import * as XTZWallet from './src/index';


async function main(): Promise<void> {
    try {
        const mnemonic: string = XTZWallet.createMnemonic(12, new Uint8Array());
        const wallet: XTZWallet.Wallet = XTZWallet.importWallet(mnemonic, { password: '', curve: 'ed25519', rpc: 'https://granadanet.smartpy.io/' });

        console.log(mnemonic);
        console.log(wallet.getSecretKey());
        console.log(wallet.getPublicKey());
        console.log(wallet.getAddress());

        const balance: string = await wallet.getBalance();
        console.log(`Balance: ${balance}`);
        
        const batch = await wallet.buildOperationBatch([
            wallet.ops.transaction('tz1ZbhR2nnmyJryyoeSWroRMhRhfoPwVcw5V', 100),
            wallet.ops.transaction('tz1ZbhR2nnmyJryyoeSWroRMhRhfoPwVcw5V', 100),
            wallet.ops.transaction('tz1ZbhR2nnmyJryyoeSWroRMhRhfoPwVcw5V', 100),
            wallet.ops.transaction('tz1ZbhR2nnmyJryyoeSWroRMhRhfoPwVcw5V', 100),
            wallet.ops.transaction('tz1ZbhR2nnmyJryyoeSWroRMhRhfoPwVcw5V', 100)
        ]);

        console.log(batch.getContents());
        console.log(batch.getEstimates());

        const signature: string = await wallet.signOperation(batch);
        console.log(signature);

        wallet.send(batch, signature).then(console.log);
    }
    catch (error: any) {
        console.error(JSON.stringify(error));
    }
}


main();