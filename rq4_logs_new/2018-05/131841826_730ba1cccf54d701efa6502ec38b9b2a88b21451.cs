using System;
using System.Collections.Generic;
using Reppertum.Core;

namespace Reppertum.Crypto
{
    public static class Merkle
    {
        public static string GetMerkleRoot(List<Transaction> txs)
        {
            List<string> layer = GetTxHashList(txs);
            
            do
            {
                layer = GetNextLayer(layer);
            } while (layer.Count != 1);
            
            return layer[0];
        }

        private static List<string> GetNextLayer(List<string> layer)
        {
            List<string> nextLayer = new List<string>();
            
            // if there are an odd number of elelemtns, duplicate the last one
            if (layer.Count % 2 != 0)
            {
                layer.Add(layer[layer.Count-1]);
            }
            
            for (int i = 0; i < layer.Count-1; i = i + 2)
            {
                nextLayer.Add(Cryptography.Sha256(layer[i] + layer[i+1]));
            }

            return nextLayer;
        }
        
        private static List<string> GetTxHashList(List<Transaction> txs)
        {
            List<string> txHashes = new List<string>();
            foreach (Transaction t in txs)
            {
                txHashes.Add(t.Hash);
            }

            return txHashes;
        }
    }
}