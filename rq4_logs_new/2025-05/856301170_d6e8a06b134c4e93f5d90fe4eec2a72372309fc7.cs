using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Devv.CloudflareDdns;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Moq.Protected;
using Xunit;

public class CloudFlareHttpClientTests
{
    [Fact]
    public async Task GetPublicIpAsync_ReturnsIp()
    {
        // Arrange
        var handlerMock = new Mock<HttpMessageHandler>();
        handlerMock
            .Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req => req.RequestUri.ToString().Contains("api.ipify.org")),
                ItExpr.IsAny<CancellationToken>()
            )
            .ReturnsAsync(new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK,
                Content = new StringContent("1.2.3.4"),
            });

        var httpClient = new HttpClient(handlerMock.Object);
        var logger = Mock.Of<ILogger<CloudFlareHttpClient>>();
        var options = Options.Create(new CloudFlareOptions { Records = new Records[0] });

        var client = new CloudFlareHttpClient(logger, httpClient, options);

        // Act
        var ip = await client.GetPublicIpAsync(CancellationToken.None);

        // Assert
        Assert.Equal("1.2.3.4", ip);
    }

    [Fact]
    public async Task UpdateDnsRecordsAsync_LogsAndCallsApi()
    {
        // Arrange
        var record = new Records { ZoneId = "zone", DnsRecordId = "dns", Name = "test.com" };
        var options = Options.Create(new CloudFlareOptions { Records = new[] { record } });
        var logger = Mock.Of<ILogger<CloudFlareHttpClient>>();

        var handlerMock = new Mock<HttpMessageHandler>();
        handlerMock
            .Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.IsAny<HttpRequestMessage>(),
                ItExpr.IsAny<CancellationToken>()
            )
            .ReturnsAsync(new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK,
                Content = new StringContent("1.2.3.4"),
            });

        var httpClient = new HttpClient(handlerMock.Object)
        {
            BaseAddress = new System.Uri("https://api.cloudflare.com")
        };

        var client = new CloudFlareHttpClient(logger, httpClient, options);

        // Act & Assert (should not throw)
        await client.UpdateDnsRecordsAsync("1.2.3.4", CancellationToken.None);
    }
}