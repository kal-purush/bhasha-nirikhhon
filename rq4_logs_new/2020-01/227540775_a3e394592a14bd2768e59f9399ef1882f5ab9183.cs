using System.Net;
using System.Threading;
using System.Threading.Tasks;
using AutoFixture.NUnit3;
using Dfe.Spi.Common.Logging.Definitions;
using Dfe.Spi.Common.WellKnownIdentifiers;
using Dfe.Spi.UkrlpAdapter.Domain.Configuration;
using Moq;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using RestSharp;

namespace Dfe.Spi.UkrlpAdapter.Infrastructure.SpiTranslator.UnitTests
{
    public class WhenTranslatingEnumValue
    {
        private Mock<IRestClient> _restClientMock;
        private TranslatorConfiguration _configuration;
        private Mock<ILoggerWrapper> _loggerMock;
        private TranslatorApiClient _translator;
        private CancellationToken _cancellationToken;

        [SetUp]
        public void Arrange()
        {
            _restClientMock = new Mock<IRestClient>();
            _restClientMock.Setup(c => c.ExecuteTaskAsync(It.IsAny<IRestRequest>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new RestResponse
                {
                    StatusCode = HttpStatusCode.OK,
                    ResponseStatus = ResponseStatus.Completed,
                    Content = GetValidResponse("Value1", new[] {"Mapped1"})
                });

            _configuration = new TranslatorConfiguration
            {
                BaseUrl = "https://translator.unit.tests",
            };

            _loggerMock = new Mock<ILoggerWrapper>();

            _translator = new TranslatorApiClient(
                _restClientMock.Object,
                _configuration,
                _loggerMock.Object);

            _cancellationToken = new CancellationToken();
        }

        [Test, AutoData]
        public async Task ThenItShouldCallEnumEndpointForUkrlp(string enumName, string sourceValue)
        {
            await _translator.TranslateEnumValue(enumName, sourceValue, _cancellationToken);

            _restClientMock.Verify(c => c.ExecuteTaskAsync(It.Is<RestRequest>(r =>
                    r.Method == Method.GET &&
                    r.Resource == $"enumerations/{enumName}/{SourceSystemNames.UkRegisterOfLearningProviders}"), _cancellationToken),
                Times.Once);
        }

        [Test, AutoData]
        public async Task ThenItShouldReturnMappingFromApiWhereAvailable(string enumName, string sourceValue,
            string sdmValue)
        {
            _restClientMock.Setup(c => c.ExecuteTaskAsync(It.IsAny<IRestRequest>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new RestResponse
                {
                    StatusCode = HttpStatusCode.OK,
                    ResponseStatus = ResponseStatus.Completed,
                    Content = GetValidResponse(sdmValue, new[] {sourceValue})
                });

            var actual = await _translator.TranslateEnumValue(enumName, sourceValue, _cancellationToken);
            Assert.AreEqual(sdmValue, actual);
        }

        [Test, AutoData]
        public async Task ThenItShouldReturnNullWhereMappingNotAvailable(string enumName, string sourceValue)
        {
            var actual = await _translator.TranslateEnumValue(enumName, sourceValue, _cancellationToken);

            Assert.IsNull(actual);
        }

        [Test]
        public void ThenItShouldThrowExceptionIfApiReturnsNonSuccess()
        {
            _restClientMock.Setup(c => c.ExecuteTaskAsync(It.IsAny<IRestRequest>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new RestResponse
                {
                    StatusCode = HttpStatusCode.InternalServerError,
                    ResponseStatus = ResponseStatus.Completed,
                });

            var actual = Assert.ThrowsAsync<TranslatorApiException>(async () =>
                await _translator.TranslateEnumValue("enum", "value", _cancellationToken));
            Assert.AreEqual(HttpStatusCode.InternalServerError, actual.StatusCode);
        }


        private string GetValidResponse(string sdmValue, string[] mappings)
        {
            var obj = new JObject(
                new JProperty("mappingsResult", new JObject(
                    new JProperty("mappings", new JObject(
                        new JProperty(sdmValue, new JArray(mappings)))))));
            return obj.ToString();
        }
    }
}