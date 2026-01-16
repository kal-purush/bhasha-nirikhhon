using CasDotnetSdk.KeyExchange;
using CasDotnetSdk.KeyExchange.Types;
using Xunit;

namespace CasDotnetSdkTests.Tests
{
    public class KeyExchangeTests
    {
        private readonly X25519Wrapper _wrapper;

        public KeyExchangeTests()
        {
            this._wrapper = new X25519Wrapper();
        }

        [Fact]
        public void DiffieHallmanExchangePass()
        {
            X25519SecretPublicKey keyPair1 = this._wrapper.GenerateSecretAndPublicKey();
            X25519SecretPublicKey keyPair2 = this._wrapper.GenerateSecretAndPublicKey();
            X25519SharedSecret shareSecret1 = this._wrapper.GenerateSharedSecret(keyPair1.SecretKey, keyPair2.PublicKey);
            X25519SharedSecret shareSecret2 = this._wrapper.GenerateSharedSecret(keyPair2.SecretKey, keyPair1.PublicKey);
            Assert.True(shareSecret1.SharedSecret.SequenceEqual(shareSecret2.SharedSecret));
        }
    }
}