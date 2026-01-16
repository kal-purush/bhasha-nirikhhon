using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using AutoFixture.NUnit3;
using Dfe.Spi.UkrlpAdapter.Domain.Configuration;
using Moq;
using NUnit.Framework;
using RestSharp;

namespace Dfe.Spi.UkrlpAdapter.Infrastructure.UkrlpSoapApi.UnitTests
{
    public class WhenGettingProvidersUpdatedSince
    {
        private Mock<IRestClient> _restClientMock;
        private Mock<IUkrlpSoapMessageBuilder> _messageBuilderMock;
        private UkrlpApiConfiguration _configuration;
        private UkrlpSoapApiClient _client;

        [SetUp]
        public void Arrange()
        {
            _restClientMock = new Mock<IRestClient>();
            _restClientMock.Setup(c => c.ExecuteTaskAsync(It.IsAny<IRestRequest>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(GetValidResponse(new []
                {
                    new ProviderTestData
                    {
                        Ukprn = 123,
                        Name = "Test",
                    }, 
                }));

            _messageBuilderMock = new Mock<IUkrlpSoapMessageBuilder>();
            _messageBuilderMock.Setup(b => b.BuildMessageToGetUpdatesSince(It.IsAny<DateTime>()))
                .Returns("some-soap-xml-request");

            _configuration = new UkrlpApiConfiguration
            {
                Url = "https://ukrlp.test.local",
                StakeholderId = "123",
            };

            _client = new UkrlpSoapApiClient(_restClientMock.Object, _messageBuilderMock.Object, _configuration);
        }

        [Test, AutoData]
        public async Task ThenItShouldBuildMessageUsingRequestUpdatedSince(DateTime updatedSince)
        {
            await _client.GetProvidersUpdatedSinceAsync(updatedSince, new CancellationToken());

            _messageBuilderMock.Verify(b => b.BuildMessageToGetUpdatesSince(updatedSince));
        }

        [Test, AutoData]
        public async Task ThenItShouldExecuteSoapRequestAgainstServer(DateTime updatedSince, string soapRequestMessage)
        {
            _messageBuilderMock.Setup(b => b.BuildMessageToGetUpdatesSince(It.IsAny<DateTime>()))
                .Returns(soapRequestMessage);

            await _client.GetProvidersUpdatedSinceAsync(updatedSince, new CancellationToken());

            var expectedSoapAction = "retrieveAllProviders";
            _restClientMock.Verify(c => c.ExecuteTaskAsync(It.Is<IRestRequest>(r =>
                    r.Method == Method.POST &&
                    r.Parameters.Any(p => p.Name == "SOAPAction") &&
                    (string) r.Parameters.Single(p => p.Name == "SOAPAction").Value == expectedSoapAction &&
                    r.Parameters.Any(p => p.Type == ParameterType.RequestBody) &&
                    r.Parameters.Single(p => p.Type == ParameterType.RequestBody).Value == soapRequestMessage),
                It.IsAny<CancellationToken>()));
        }

        [Test, AutoData]
        public async Task ThenItShouldReturnDeserializedProviders(DateTime updatedSince, ProviderTestData[] returnedProviders)
        {
            _restClientMock.Setup(c => c.ExecuteTaskAsync(It.IsAny<IRestRequest>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(GetValidResponse(returnedProviders));

            var actual = await _client.GetProvidersUpdatedSinceAsync(updatedSince, new CancellationToken());

            Assert.IsNotNull(actual);
            Assert.AreEqual(returnedProviders.Length, actual.Length);
            for (var i = 0; i < returnedProviders.Length; i++)
            {
                Assert.AreEqual(returnedProviders[i].Ukprn, actual[i].UnitedKingdomProviderReferenceNumber,
                    $"Expected item {i} to have Ukprn {returnedProviders[i].Ukprn} but has {actual[i].UnitedKingdomProviderReferenceNumber}");
                Assert.AreEqual(returnedProviders[i].Name, actual[i].ProviderName,
                    $"Expected item {i} to have Name {returnedProviders[i].Name} but has {actual[i].ProviderName}");
                Assert.AreEqual(returnedProviders[i].Postcode, actual[i].Postcode,
                    $"Expected item {i} to have Postcode {returnedProviders[i].Postcode} but has {actual[i].Postcode}");
            }
        }

        [Test, AutoData]
        public async Task ThenItShouldReturnEmptyArrayIfNoProvidersFound(DateTime updatedSince)
        {
            _restClientMock.Setup(c => c.ExecuteTaskAsync(It.IsAny<IRestRequest>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(GetEmptyResponse());

            var actual = await _client.GetProvidersUpdatedSinceAsync(updatedSince, new CancellationToken());

            Assert.IsNotNull(actual);
            Assert.AreEqual(0, actual.Length);
        }

        [Test, AutoData]
        public void ThenItShouldThrowExceptionIfSoapFaultReceived(DateTime updatedSince, string faultCode, string faultString)
        {
            _restClientMock.Setup(c => c.ExecuteTaskAsync(It.IsAny<IRestRequest>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(GetFaultResponse(faultCode, faultString));

            var actual = Assert.ThrowsAsync<SoapException>(async () =>
                await _client.GetProvidersUpdatedSinceAsync(updatedSince, new CancellationToken()));
            Assert.AreEqual(faultCode, actual.FaultCode);
            Assert.AreEqual(faultString, actual.Message);
        }


        private XNamespace soapNs = "http://schemas.xmlsoap.org/soap/envelope/";

        private IRestResponse GetValidResponse(ProviderTestData[] providers)
        {
            XNamespace ukrlpNs = "http://ukrlp.co.uk.server.ws.v3";

            var providerElements = providers.Select(p =>
            {
                var providerElement = new XElement("MatchingProviderRecords",
                    new XElement("UnitedKingdomProviderReferenceNumber", p.Ukprn),
                    new XElement("ProviderName", p.Name));
                if (!string.IsNullOrEmpty(p.Postcode))
                {
                    providerElement.Add(new XElement("ProviderContact",
                        new XElement("ContactType", "L"),
                        new XElement("ContactAddress",
                            new XElement("PostCode", p.Postcode))));
                }

                return providerElement;
            }).ToArray();
            
            var envelope = GetSoapEnvelope(new XElement(ukrlpNs + "ProviderQueryResponse",
                new XAttribute(XNamespace.Xmlns + "ukrlp", ukrlpNs.NamespaceName),
                providerElements));

            var responseMock = new Mock<IRestResponse>();
            responseMock.Setup(r => r.Content).Returns(envelope.ToString());
            responseMock.Setup(r => r.IsSuccessful).Returns(true);
            return responseMock.Object;
        }

        private IRestResponse GetEmptyResponse()
        {
            XNamespace ukrlpNs = "http://ukrlp.co.uk.server.ws.v3";

            var envelope = GetSoapEnvelope(new XElement(ukrlpNs + "ProviderQueryResponse",
                new XAttribute(XNamespace.Xmlns + "ukrlp", ukrlpNs.NamespaceName)));

            var responseMock = new Mock<IRestResponse>();
            responseMock.Setup(r => r.Content).Returns(envelope.ToString());
            responseMock.Setup(r => r.IsSuccessful).Returns(true);
            return responseMock.Object;
        }

        private IRestResponse GetFaultResponse(string faultCode, string faultString)
        {
            var envelope = GetSoapEnvelope(new XElement(soapNs + "Fault",
                new XElement("faultcode", faultCode),
                new XElement("faultstring", faultString)));

            var responseMock = new Mock<IRestResponse>();
            responseMock.Setup(r => r.Content).Returns(envelope.ToString());
            responseMock.Setup(r => r.IsSuccessful).Returns(false);
            return responseMock.Object;
        }

        private XElement GetSoapEnvelope(XElement bodyContent)
        {
            return new XElement(soapNs + "Envelope",
                new XAttribute(XNamespace.Xmlns + "soapenv", soapNs.NamespaceName),
                new XElement(soapNs + "Body",
                    bodyContent));
        }
        
        public class ProviderTestData
        {
            public long Ukprn { get; set; }
            public string Name { get; set; }
            public string Postcode { get; set; }
        }
    }
}