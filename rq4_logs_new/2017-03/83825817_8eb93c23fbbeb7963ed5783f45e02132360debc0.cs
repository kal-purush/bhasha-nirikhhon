using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace BlockchainHelloWorldClasses.Block.Block {
    public class BlockData:BlockBase {

        private string Data;

        public BlockData(int i) : base(i) {}

        /// <summary>
        /// Setter
        /// </summary>
        public void AddData(string d) {
            Data = d;
        }

        /// <summary>
        /// Getter
        /// </summary>
        public string GetData() {
            return Data;
        }

        /// <summary>
        /// ToString() includes data field
        /// </summary>
        /// <returns>Single Block or Blockchain if it is not Genesis block.</returns>
        public override string ToString() {

            // Genesis Block:
            String reviousString = "null";

            // If not Genesis Block:
            if (PreviousBlock != null)
                reviousString = PreviousBlock.ToString();

            // Add Previous Block (BlockChain!):
            return "{id:" + Id + "," +
                    "nonce:" + Nonce + "," +
                    "data:\"" + Data + "\"," +
                    "previous:" + reviousString.ToString() +
                    "}";

        }

    }
}