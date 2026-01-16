using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Security.Cryptography;
using System.Text;

namespace BlockchainHelloWorldTest {
    [TestClass]
    public class EncryptionTest {
        
        
        /// <summary>
        /// Basic SHA1 script in procedural format;
        ///   REFERENCES:
        /// - https://msdn.microsoft.com/en-us/library/system.security.cryptography.sha1(v=vs.110).aspx
        /// - https://gist.github.com/kristopherjohnson/3021045
        /// </summary>
        [TestMethod]
        public void TestBasicCrypto() {


            //byte[] data = new byte[256];
            byte[] data = Encoding.UTF8.GetBytes("Hello World");
            byte[] result;

            SHA1 sha = new SHA1CryptoServiceProvider();
            // This is one implementation of the abstract class SHA1.
            result = sha.ComputeHash(data);

            // convert the System.Byte[] to string:
            var sb = new StringBuilder();
            foreach (byte b in result) {
                var hex = b.ToString("x2");
                sb.Append(hex);
            }

            //Assert:
            Assert.AreEqual("0a4d55a8d778e5022fab701977c5d840bbc486d0", sb.ToString());

        }
    }
}