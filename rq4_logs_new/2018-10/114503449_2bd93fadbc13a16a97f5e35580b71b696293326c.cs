using System;
using System.Net;
using System.Net.Http;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc.Testing;
using TopDownHeist.Server;
using Xunit;

namespace TopDownHeist.IntegrationTests
{
    public class BasicTests : IClassFixture<WebApplicationFactory<Startup>>
    {
        private readonly WebApplicationFactory<Startup> _webApplicationFactory;
        private readonly HttpClient _httpClient;

        public BasicTests(WebApplicationFactory<Startup> webApplicationFactory)
        {
            _webApplicationFactory = webApplicationFactory;
            _httpClient = _webApplicationFactory.CreateClient();
        }

        [Fact]
        public async Task IndexReturnsAsync()
        {
            var httpClient = _webApplicationFactory.CreateClient();

            var response = await httpClient.GetAsync("/");
            response.EnsureSuccessStatusCode();

            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }

        [Fact]
        public async Task WebSocketConnectDisconnect()
        {
            var socketClient = _webApplicationFactory.Server.CreateWebSocketClient(); ;

            var socket = await socketClient.ConnectAsync(new Uri("http://localhost/heistHub"), default(CancellationToken));

            Assert.Equal(WebSocketState.Open, socket.State);

            await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Client disconnected normally.", default(CancellationToken));

            Assert.Equal(WebSocketState.Closed, socket.State);
        }
    }
}