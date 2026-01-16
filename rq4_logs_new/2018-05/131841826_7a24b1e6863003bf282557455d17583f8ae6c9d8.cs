using System;

namespace Reppertum
{
    public class Consensus
    {
        public static bool ProofOfWork(Block prevB, Block newB, UInt16 difficulty = 5)
        {
            Console.Clear();
            Console.WriteLine("Computing Proof-of-Work...");
            bool valid = false;
            UInt32 nonce = 0;
            string header = newB.Header.Index + newB.Header.PreviousHash;
            string hash;
            string diff = string.Empty;
            for (int i = 0; i < difficulty-1; i++)
            {
                diff += "0";
            }
            
            do 
            {
                hash = Cryptography.Sha256(header + nonce);
                nonce++;
            } 
            while (hash.Substring(0, difficulty - 1) != diff);
            
            valid = (hash.Substring(0, difficulty-1) == diff);
            
            Console.WriteLine("Successfully computed!");
            
            return valid;
        }
    }
}