using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;

namespace Reppertum
{
    public class Blockchain
    {
        public readonly List<Block> Chain = new List<Block>();
        public readonly string FirstHash;
        
        private UInt16 _current = 0;

        public Blockchain(string prevHash, string data, Int64 timestamp) {
            string hash = GetHash(_current, prevHash, data, timestamp);
            FirstHash = hash;
            Block b = new Block(_current, prevHash, hash, data, timestamp);
            Chain.Add(b);
            _current++;
        }

        public Block AddBlock(string prevHash, string data, Int64 timestamp) {
            Block prevB = Chain[_current-1];
            string hash = GetHash(_current, prevHash, data, timestamp);
            Block newB = new Block(_current, prevHash, hash, data, timestamp);

            if (ProofOfWork((Block)prevB, newB)) {
                Chain.Add(newB);
                _current++;
            }
            else if (prevB.PreviousHash != prevHash && prevB.Index != 0) {
                throw new Exception("Hashes do not match");
            }
            else {
                throw new Exception("Chain not valid");
            }

            return newB;
        }

        public Block GetBlock(UInt16 index) {
            return Chain[index];
        }

        public string GetHash(UInt16 i, string prevHash, string data, Int64 timestamp) {
            return Hash(i.ToString() + prevHash + data + timestamp);
        }

        public Int32 GetNumberOfBlocks() {
            return Chain.Count;
        }

        private static string Hash(string str) {
            SHA256Managed crypt = new SHA256Managed();
            StringBuilder hash = new StringBuilder();
            Byte[] crypto = crypt.ComputeHash(Encoding.UTF8.GetBytes(str));
            foreach (Byte theByte in crypto)
            {
                hash.Append(theByte.ToString("x2"));
            }
            return hash.ToString();
        }

        private static bool ProofOfWork(Block prevB, Block newB, UInt16 difficulty = 5)
        {
            Console.WriteLine("Computing Proof-of-Work...");
            bool valid = false;
            UInt32 nonce = 0;
            string header = newB.Index + newB.PreviousHash;
            string hash;
            do {
                hash = Hash(header + nonce);
                nonce++;
            } while (hash.Substring(0, difficulty - 1) != "0000");
            
            valid = (hash.Substring(0, difficulty-1) == "0000") ? true : false;
            
            Console.WriteLine("Successfully computed!");
            
            return valid;
        }
    }
}