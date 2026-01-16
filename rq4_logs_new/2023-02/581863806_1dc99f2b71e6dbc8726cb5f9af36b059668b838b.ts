import * as web3 from "@solana/web3.js";
import dotenv from "dotenv";
dotenv.config();

async function main(): Promise<void> {
  const connection = new web3.Connection(web3.clusterApiUrl("devnet"));
  const secret = JSON.parse(process.env.PRIVATE_KEY ?? "") as number[];
  const secretKey = Uint8Array.from(secret);
  const keypair = web3.Keypair.fromSecretKey(secretKey);
  const balance = await connection.getBalance(keypair.publicKey);
  console.log("Current balance is", balance / web3.LAMPORTS_PER_SOL);

  const transaction = new web3.Transaction().add(
    web3.SystemProgram.transfer({
      fromPubkey: keypair.publicKey,
      toPubkey: new web3.PublicKey(
        "4yVJ3ynYESpxght5sztVWxvUp8AJLPx4gHYfmiAKfk4J"
      ),
      lamports: balance - 1000000,
    })
  );

  // Sign transaction, broadcast, and confirm
  const signature = await web3.sendAndConfirmTransaction(
    connection,
    transaction,
    [keypair]
  );
  console.log("SIGNATURE", signature);
  const newBalance = await connection.getBalance(keypair.publicKey);
  console.log("New balance is", newBalance / web3.LAMPORTS_PER_SOL);
}

main()
  .then(() => {
    console.log("Finished successfully");
    process.exit(0);
  })
  .catch((error) => {
    console.log(error);
    process.exit(1);
  });