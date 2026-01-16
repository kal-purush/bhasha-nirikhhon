using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using legallead.records.search.Classes;

namespace legallead.records.search.Tests
{
    [TestClass]
    public class CryptoEngineTests
    {
        /// <summary>
        /// This method is used to generate keys that can be read from the application configuration file
        /// The example shown is used to generate system, password credential
        /// The convention used is to separate uid and pwd with a pipe | delimiter
        /// </summary>
        [TestMethod]
        public void CanBlackBox()
        {
            // dev.support.ticket@transcore.com
            const string saltLocal = "email.send.items";
            var toBeEncoded = "adm.thompson.recordsearch@gmail.com|234-record-search-432";
            var pwd = CryptoEngine.Encrypt(toBeEncoded, saltLocal);
            Console.WriteLine(string.Format("pwd: {0}", pwd));
            var decoded = CryptoEngine.Decrypt(pwd, saltLocal);
            Assert.IsTrue(decoded.Equals(toBeEncoded, StringComparison.CurrentCulture));
        }
    }
}