using BlockchainHelloWorld.Block.Encryption;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace BlockchainHelloWorld.Block.Miner {
    public class MinerBase {

        private BlockBase Block;
        private int Difficulty;


        /// <summary>
        /// Setters
        /// </summary>
        public void SetBlock(BlockBase b){
            Block = b;
        }

        public void SetDifficulty(int i) {
            Difficulty = i;
        }


        /// <summary>
        /// Getters
        /// </summary>
        public BlockBase GetBlock() {
            return Block;
        }




        public int Mine(int starting = 0, int ending = 1000000) {

            EncryptionBlock e = new EncryptionBlock();

            for (int i = starting; i <= 1000000; i++) {
                
                //Set the nonce on the block:
                Block.SetNonce(i);

                //hash the block:
                String hash = e.Encrypt(Block);

                if (CompareHashAgainstDifficulty(hash))
                    return i;
            }

            // no result found:
            throw new Exception("No result found between [" + starting + "]-[" + ending + "] with a difficulty of [" + Difficulty + "]");

        }


        /// <summary>
        /// Check the hash against the difficulty. Right now the difficulty level is done with leading zeros
        /// 1 = 0, 2 = 00, 3 = 000, etc. 
        /// @todo: make this a bit more sophisticated
        /// </summary>
        /// <param name="hash"></param>
        /// <returns></returns>
        private bool CompareHashAgainstDifficulty(String hash) {

            string stringDifficulty = "";
            for (int i = 0; i < Difficulty; i++) {
                stringDifficulty += "0";
            }

            if (hash.StartsWith(stringDifficulty))
                return true;

            return false;
        }

    }
}