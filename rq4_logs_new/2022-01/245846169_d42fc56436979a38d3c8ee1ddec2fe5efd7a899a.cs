using Microsoft.Extensions.Logging;
using Xunit;

namespace sbroennelab.nhkworldtv.Tests
{
    /// <summary>
    /// Tests for creating the JSON Blob on Azure Storage
    /// </summary>

    public class Test_JsonBlob
    {
        private ILogger logger = (ListLogger)TestFactory.CreateLogger(LoggerTypes.List);


        [Fact]
        public async void Test_CreateJsonBlob()
        {
            bool success = await JsonBlob.Create(logger);
            Assert.True(success);
        }
    }
}
