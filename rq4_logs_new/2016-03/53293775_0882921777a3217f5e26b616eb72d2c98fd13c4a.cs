using System.Configuration;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Scrumfish.WebApi.v2.ApiSite.Tests
{
    [TestClass]
    [Ignore]
    public class ApiSite_Integration_Tests
    {
        private HttpWebRequest _target;

        [TestInitialize]
        public void Setup()
        {
            // If you are testing against a self-signed cert, use this.
            //ServicePointManager.ServerCertificateValidationCallback =
            //    (object o, X509Certificate c, X509Chain ch, SslPolicyErrors e) => true;

            _target = WebRequest.CreateHttp(ConfigurationManager.AppSettings["uri"]);
            _target.Method = "GET";
            _target.Accept = "application/json";
        }

        [TestMethod]
        [TestCategory("Integration")]
        [ExpectedException(typeof(System.Net.WebException))]
        public void Get_FailsWithNoCert_Test()
        {
            using (var response = _target.GetResponse() as HttpWebResponse)
            {
                Assert.Fail("Should have gotten an exception.");
            }
        }

        [TestMethod]
        [TestCategory("Integration")]
        public void Get_ReceivesOkWithValidCert_Test()
        {
            _target.ClientCertificates.Add(new X509Certificate2("valid.example.com.pfx", "password"));
            using (var response = _target.GetResponse() as HttpWebResponse)
            {
                Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            }
        }

        [TestMethod]
        [TestCategory("Integration")]
        public void Get_ReceivesOkWithValidCertThumbprint_Test()
        {
            _target.ClientCertificates.Add(new X509Certificate2("thumbprint.example.com.pfx", "password"));
            using (var response = _target.GetResponse() as HttpWebResponse)
            {
                Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            }
        }

        [TestMethod]
        [TestCategory("Integration")]
        public void Get_ReceivesUnauthorizedWithExpiredCert_Test()
        {
            _target.ClientCertificates.Add(new X509Certificate2("expired.example.com.pfx", "password"));
            try
            {
                using (var response = _target.GetResponse() as HttpWebResponse)
                {
                    Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
                }
            }
            catch (WebException exception)
            {
                using (var caughtResponse = exception.Response as HttpWebResponse)
                {
                    Assert.AreEqual(HttpStatusCode.Unauthorized, caughtResponse.StatusCode);
                }
            }
        }

        [TestMethod]
        [TestCategory("Integration")]
        public void Get_ReceivesUnauthorizedWithUnusedCert_Test()
        {
            _target.ClientCertificates.Add(new X509Certificate2("unused.example.com.pfx", "password"));
            try
            {
                using (var response = _target.GetResponse() as HttpWebResponse)
                {
                    Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
                }
            }
            catch (WebException exception)
            {
                using (var caughtResponse = exception.Response as HttpWebResponse)
                {
                    Assert.AreEqual(HttpStatusCode.Unauthorized, caughtResponse.StatusCode);
                }
            }
        }
    }
}