using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Web;

namespace BlockchainHelloWorld {
    public class Encryption {


        public string Encrypt(String s) {
            return ConvertToString(Sha1(s)); ;
        }

        /// <summary>
        /// Convert String to sha1
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        private byte[] Sha1(String s) {

            // convert string to bytes:
            byte[] data = Encoding.UTF8.GetBytes(s);

            // create sha instance:
            SHA1 sha = new SHA1CryptoServiceProvider();
            
            // return computedhash of s
            return sha.ComputeHash(data);
        }

        /// <summary>
        /// Converts byte[] to String of sha1
        /// </summary>
        /// <param name="result">Hash of the incoming string</param>
        /// <returns></returns>
        private String ConvertToString(byte[] result) {
            var sb = new StringBuilder();
            foreach (byte b in result) {
                var hex = b.ToString("x2");
                sb.Append(hex);
            }
            return sb.ToString();
        }
    }
}