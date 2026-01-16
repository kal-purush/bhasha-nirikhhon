using BlockchainHelloWorldClasses.Block.Block;
using BlockchainHelloWorldClasses.Block.Encryption;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace BlockchainHelloWorldClasses.Block.Miner {
    public class MinerData:MinerBase {

        new protected BlockData Blockchain;

        /// <summary>
        /// Constructor. Create the Genesis block
        /// </summary>
        public MinerData() {
            Blockchain = new BlockData(0);
            Blockchain.SetData("Hello Genesis Block!");
        }

        new public BlockData GetBlockchain() {
            return Blockchain;
        }

        public void Mine(BlockData b, int starting = 0, int ending = 1000000) {
            EncryptionBlock e = new EncryptionBlock();

            for (int i = starting; i <= 1000000; i++) {

                //Set the nonce on the block:
                b.SetNonce(i);

                //hash the block:
                String hash = e.Encrypt(b);

                // if hash found:
                if (CompareHashAgainstDifficulty(hash)) {
                    //attach the previous blocks to this block:
                    b.SetPreviousBlock(Blockchain);

                    //set this block as the current blockchain:
                    Blockchain = b;

                    //return something:
                    return;

                }

            }

            // no result found:
            throw new Exception("No result found between [" + starting + "]-[" + ending + "] with a difficulty of [ unknown ]");
        }
    }
}