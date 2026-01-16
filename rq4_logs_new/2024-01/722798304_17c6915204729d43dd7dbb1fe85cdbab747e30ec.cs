using CasDotnetSdk.Hashers;
using System.Text;
using Xunit;

namespace CasDotnetSdkTests.Tests
{
    public class SHAWrapperTests
    {
        private SHAWrapper _wrapper;
        private string _testString;
        public SHAWrapperTests()
        {
            this._wrapper = new SHAWrapper();
            this._testString = "Test hash to hash";
        }

        [Fact]
        public void SHA512HashBytes()
        {
            byte[] data = Encoding.UTF8.GetBytes(this._testString);
            byte[] hashed = this._wrapper.Hash512(data);
            Assert.NotNull(hashed);
            Assert.NotEmpty(hashed);
            Assert.True(hashed.Length > 0);
        }

        [Fact]
        public void SHA512VerifyPass()
        {
            byte[] data = Encoding.UTF8.GetBytes(this._testString);
            byte[] hashed = this._wrapper.Hash512(data);
            bool isSame = this._wrapper.Verify512(data, hashed);
            Assert.True(isSame);
        }

        [Fact]
        public void SHA512VerifyFail()
        {
            byte[] data = Encoding.UTF8.GetBytes(this._testString);
            byte[] hashed = this._wrapper.Hash512(data);
            data = Encoding.UTF8.GetBytes("Not the same byte array");
            bool isSame = this._wrapper.Verify512(data, hashed);
            Assert.False(isSame);
        }

        [Fact]
        public void SHA256HashBytes()
        {
            byte[] data = Encoding.UTF8.GetBytes(this._testString);
            byte[] hashed = this._wrapper.Hash256(data);
            Assert.NotNull(hashed);
            Assert.NotEmpty(hashed);
            Assert.True(hashed.Length > 0);
        }

        [Fact]
        public void SHA256VerifyPass()
        {
            byte[] data = Encoding.UTF8.GetBytes(this._testString);
            byte[] hashed = this._wrapper.Hash256(data);
            bool isSame = this._wrapper.Verify256(data, hashed);
            Assert.True(isSame);
        }

        [Fact]
        public void SHA256VerifyFail()
        {
            byte[] data = Encoding.UTF8.GetBytes(this._testString);
            byte[] hashed = this._wrapper.Hash256(data);
            data = Encoding.UTF8.GetBytes("Not the same byte array");
            bool isSame = this._wrapper.Verify256(data, hashed);
            Assert.False(isSame);
        }
    }
}