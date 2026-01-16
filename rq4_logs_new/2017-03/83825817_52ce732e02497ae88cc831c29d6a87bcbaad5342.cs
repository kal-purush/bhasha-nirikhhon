using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Web;

namespace BlockchainHelloWorld.Encryption {
    public class EncryptionBlock:EncryptionBase {

        public string Encrypt(Block o) {

            byte[] b = ToByte(o);
            byte[] sha = ByteToSha1(b);
            return ShaToString(sha);

        }

        /// <summary>
        /// Converts Block to String and returns byte array.
        /// @todo look to see if there is a better way to handle this.
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        private byte[] ToByte(Block obj) {
            return Encoding.UTF8.GetBytes(obj.ToString()); 
        }

    }
}