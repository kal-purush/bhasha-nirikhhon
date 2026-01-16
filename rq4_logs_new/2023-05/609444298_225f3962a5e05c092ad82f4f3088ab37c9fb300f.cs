using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.VisualStudio.TestPlatform.TestHost;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace jiraF.Stage.EndToEndTests
{
    public class PingTests : IDisposable
    {
        private readonly HttpClient _client;

        public PingTests()
        {
            var application = new WebApplicationFactory<Program>();
            _client = application.CreateClient();
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
        }

        [Theory]
        [InlineData("/ping")]
        public async Task ping_CheckPing_StatusCode200(string url)
        {
            HttpResponseMessage response = await _client.GetAsync(url);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }
    }
}