using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace BlockchainHelloWorld {
    public class Block:Object {

        private int id;

        /// <summary>
        /// Constructor
        /// </summary>
        public Block() {
            id = 0;
        }

        /// <summary>
        /// Setters
        /// </summary>
        public void SetId(int i){
            id = i;
        }

        /// <summary>
        /// Getters
        /// </summary>
        public int GetId() {
            return id;
        }

    }
}