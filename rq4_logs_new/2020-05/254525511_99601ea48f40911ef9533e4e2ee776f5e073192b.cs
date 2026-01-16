using System;
using System.Threading.Tasks;
using Xunit;

namespace Remoteit.Test.RestApi
{
    public class RemoteitApiRequest
    {
        // Test Cases
        // 1. When IsSuccessStatusCode is false, the correct AuthenticationException is thrown.
        // 2. When an HttpRequestException us thrown, the correct AuthenticationException is thrown.

        [Fact]
        public async Task TestHandlingIncorrectStatusCode()
        {
            throw new NotImplementedException();
        }

        [Fact]
        public async Task TestHandlingHttpRequestException()
        {
            throw new NotImplementedException();
        }
    }
}