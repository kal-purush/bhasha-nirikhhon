using CasDotnetSdk.Asymmetric;
using CasDotnetSdk.DigitalSignature;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using static CasDotnetSdk.DigitalSignature.DigitalSignatureWrapper;

namespace CasDotnetSdkTests.Tests
{
    public class DigitalSignatureTests
    {
        private readonly DigitalSignatureWrapper _digitalSignatureWrapper;
        
        public DigitalSignatureTests()
        {
            this._digitalSignatureWrapper = new DigitalSignatureWrapper();
        }

        [Fact]
        public void RSA4096DigitalSignature()
        {
            byte[] dataToSign = Encoding.UTF8.GetBytes("WelcomeHomeToSigningData");
            SHARSADigitalSignatureResult signature = this._digitalSignatureWrapper.SHARSADigitalSignature(4096, dataToSign);
            RSAWrapper rsaWrapper = new RSAWrapper();
            bool result = rsaWrapper.RsaVerifyBytes(signature.PublicKey, dataToSign, signature.Signature);
            Assert.True(result);
        }
    }
}