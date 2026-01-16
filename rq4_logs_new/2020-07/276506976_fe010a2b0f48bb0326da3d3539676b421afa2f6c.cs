using System;
using NetStandardTestHelper.Xunit;
using Xunit;

namespace HttpHelper.Test
{
    /// <summary>
    /// Test the <see cref="HttpHelper"/> class
    /// </summary>
    public class HttpHelper_Tests
    {
        #region Constructor

        /// <summary>
        /// Test that constructor throws on null logger
        /// </summary>
        [Fact]
        public void Constructor_Base_Null_Logger()
        {
            var ex = Assert.Throws<ArgumentNullException>(() => new HttpHelper(logger: TestValues.Logger_Null));

            TestHelper.AssertArgumentNullException(ex,
                "logger");
        }

        /// <summary>
        /// Test that constructor throws on null logger
        /// </summary>
        [Fact]
        public void Constructor_Basic_Null_Logger()
        {
            var ex = Assert.Throws<ArgumentNullException>(() => new HttpHelper(logger: TestValues.Logger_Null,
                username: TestValues.Username,
                password: TestValues.Password));

            TestHelper.AssertArgumentNullException(ex,
                "logger");
        }

        /// <summary>
        /// Test that constructor throws on null username
        /// </summary>
        [Fact]
        public void Constructor_Basic_Null_Username()
        {
            var ex = Assert.Throws<ArgumentNullException>(() => new HttpHelper(logger: TestValues.Logger_Mock,
                username: NetStandardTestHelper.TestValues.StringEmpty,
                password: TestValues.Password));

            TestHelper.AssertArgumentNullException(ex,
                "username");
        }

        /// <summary>
        /// Test that constructor throws on null password
        /// </summary>
        [Fact]
        public void Constructor_Basic_Null_Password()
        {
            var ex = Assert.Throws<ArgumentNullException>(() => new HttpHelper(logger: TestValues.Logger_Mock,
                username: TestValues.Username,
                password: NetStandardTestHelper.TestValues.StringEmpty));

            TestHelper.AssertArgumentNullException(ex,
                "password");
        }

        /// <summary>
        /// Test that constructor throws on null logger
        /// </summary>
        [Fact]
        public void Constructor_ClientCredentials_Null_Logger()
        {
            var ex = Assert.Throws<ArgumentNullException>(() => new HttpHelper(logger: TestValues.Logger_Null,
                clientId: TestValues.ClientId,
                clientSecret: TestValues.ClientSecret,
                authUrl: TestValues.AuthUrl));

            TestHelper.AssertArgumentNullException(ex,
                "logger");
        }

        /// <summary>
        /// Test that constructor throws on null client id
        /// </summary>
        [Fact]
        public void Constructor_ClientCredentials_Null_ClientId()
        {
            var ex = Assert.Throws<ArgumentNullException>(() => new HttpHelper(logger: TestValues.Logger_Mock,
                clientId: NetStandardTestHelper.TestValues.StringEmpty,
                clientSecret: TestValues.ClientSecret,
                authUrl: TestValues.AuthUrl));

            TestHelper.AssertArgumentNullException(ex,
                "clientId");
        }

        /// <summary>
        /// Test that constructor throws on null client secret
        /// </summary>
        [Fact]
        public void Constructor_ClientCredentials_Null_ClientSecret()
        {
            var ex = Assert.Throws<ArgumentNullException>(() => new HttpHelper(logger: TestValues.Logger_Mock,
                clientId: TestValues.ClientId,
                clientSecret: NetStandardTestHelper.TestValues.StringEmpty,
                authUrl: TestValues.AuthUrl));

            TestHelper.AssertArgumentNullException(ex,
                "clientSecret");
        }

        /// <summary>
        /// Test that constructor throws on null logger
        /// </summary>
        [Fact]
        public void Constructor_ClientCredentials_Null_AuthUrl()
        {
            var ex = Assert.Throws<ArgumentNullException>(() => new HttpHelper(logger: TestValues.Logger_Mock,
                clientId: TestValues.ClientId,
                clientSecret: TestValues.ClientSecret,
                authUrl: NetStandardTestHelper.TestValues.StringEmpty));

            TestHelper.AssertArgumentNullException(ex,
                "authUrl");
        }

        #endregion Constructor

        #region DeleteAsync

        /// <summary>
        /// Test that function throws on null url
        /// </summary>
        [Fact]
        public async void DeleteAsync_Null_Url()
        {
            var ex = await Assert.ThrowsAsync<ArgumentNullException>(() => TestValues.HttpHelper.DeleteAsync(url: NetStandardTestHelper.TestValues.StringEmpty));

            TestHelper.AssertArgumentNullException(ex,
                "url");
        }

        #endregion DeleteAsync

        #region GetAsync

        /// <summary>
        /// Test that function throws on null url
        /// </summary>
        [Fact]
        public async void GetAsync_Null_Url()
        {
            var ex = await Assert.ThrowsAsync<ArgumentNullException>(() => TestValues.HttpHelper.GetAsync(url: NetStandardTestHelper.TestValues.StringEmpty));

            TestHelper.AssertArgumentNullException(ex,
                "url");
        }

        #endregion GetAsync

        #region GetOAuth2ClientCredentialsAsync

        /// <summary>
        /// Test that function throws on null client id
        /// </summary>
        [Fact]
        public async void GetOAuth2ClientCredentials_Null_ClientId()
        {
            var ex = await Assert.ThrowsAsync<ArgumentNullException>(() => TestValues.HttpHelper.GetOAuth2ClientCredentialsAsync(clientId: NetStandardTestHelper.TestValues.StringEmpty,
                clientSecret: TestValues.ClientSecret,
                authUrl: TestValues.AuthUrl));

            TestHelper.AssertArgumentNullException(ex,
                "clientId");
        }

        /// <summary>
        /// Test that function throws on null client secret
        /// </summary>
        [Fact]
        public async void GetOAuth2ClientCredentials_Null_ClientSecret()
        {
            var ex = await Assert.ThrowsAsync<ArgumentNullException>(() => TestValues.HttpHelper.GetOAuth2ClientCredentialsAsync(clientId: TestValues.ClientId,
                clientSecret: NetStandardTestHelper.TestValues.StringEmpty,
                authUrl: TestValues.AuthUrl));

            TestHelper.AssertArgumentNullException(ex,
                "clientSecret");
        }

        /// <summary>
        /// Test that function throws on null client auth url
        /// </summary>
        [Fact]
        public async void GetOAuth2ClientCredentials_Null_AuthUrl()
        {
            var ex = await Assert.ThrowsAsync<ArgumentNullException>(() => TestValues.HttpHelper.GetOAuth2ClientCredentialsAsync(clientId: TestValues.ClientId,
                clientSecret: TestValues.ClientSecret,
                authUrl: NetStandardTestHelper.TestValues.StringEmpty));

            TestHelper.AssertArgumentNullException(ex,
                "authUrl");
        }

        #endregion GetOAuth2ClientCredentialsAsync

        #region PatchAsync

        /// <summary>
        /// Test that function throws on null url
        /// </summary>
        [Fact]
        public async void PatchAsync_Null_Url()
        {
            var ex = await Assert.ThrowsAsync<ArgumentNullException>(() => TestValues.HttpHelper.PatchAsync(url: NetStandardTestHelper.TestValues.StringEmpty,
                body: TestValues.Body));

            TestHelper.AssertArgumentNullException(ex,
                "url");
        }

        /// <summary>
        /// Test that function throws on null body
        /// </summary>
        [Fact]
        public async void PatchAsync_Null_Body()
        {
            var ex = await Assert.ThrowsAsync<ArgumentNullException>(() => TestValues.HttpHelper.PatchAsync(url: TestValues.Url,
                body: NetStandardTestHelper.TestValues.StringEmpty));

            TestHelper.AssertArgumentNullException(ex,
                "body");
        }

        #endregion PatchAsync

        #region PostAsync

        /// <summary>
        /// Test that function throws on null url
        /// </summary>
        [Fact]
        public async void PostAsync_Null_Url()
        {
            var ex = await Assert.ThrowsAsync<ArgumentNullException>(() => TestValues.HttpHelper.PostAsync(url: NetStandardTestHelper.TestValues.StringEmpty,
                body: TestValues.Body));

            TestHelper.AssertArgumentNullException(ex,
                "url");
        }

        /// <summary>
        /// Test that function throws on null body
        /// </summary>
        [Fact]
        public async void PostAsync_Null_Body()
        {
            var ex = await Assert.ThrowsAsync<ArgumentNullException>(() => TestValues.HttpHelper.PostAsync(url: TestValues.Url,
                body: NetStandardTestHelper.TestValues.StringEmpty));

            TestHelper.AssertArgumentNullException(ex,
                "body");
        }

        #endregion PostAsync

        #region PutAsync

        /// <summary>
        /// Test that function throws on null url
        /// </summary>
        [Fact]
        public async void PutAsync_Null_Url()
        {
            var ex = await Assert.ThrowsAsync<ArgumentNullException>(() => TestValues.HttpHelper.PutAsync(url: NetStandardTestHelper.TestValues.StringEmpty,
                body: TestValues.Body));

            TestHelper.AssertArgumentNullException(ex,
                "url");
        }

        /// <summary>
        /// Test that function throws on null body
        /// </summary>
        [Fact]
        public async void PutAsync_Null_Body()
        {
            var ex = await Assert.ThrowsAsync<ArgumentNullException>(() => TestValues.HttpHelper.PutAsync(url: TestValues.Url,
                body: NetStandardTestHelper.TestValues.StringEmpty));

            TestHelper.AssertArgumentNullException(ex,
                "body");
        }

        #endregion PutAsync
    }
}