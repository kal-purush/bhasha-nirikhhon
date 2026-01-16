// <copyright file="CallbackNoneTest.cs" company="JSSoft">
//   Copyright (c) 2024 Jeesu Choi. All Rights Reserved.
//   Licensed under the MIT License. See LICENSE.md in the project root for license information.
// </copyright>

using JSSoft.Communication.Tests.Extensions;
using Xunit.Abstractions;

namespace JSSoft.Communication.Tests;

public class CallbackNoneTest : IAsyncLifetime
{
    private const int Timeout = 3000;
    private readonly ITestOutputHelper _logger;
    private readonly TestServer1 _testServer = new();
    private readonly ServerContext _serverContext;
    private readonly ClientContext _clientContext;
    private readonly RandomEndPoint _endPoint = new();
    private ITestService1? _server;

    private Guid _clientToken;
    private Guid _serverToken;

    public CallbackNoneTest(ITestOutputHelper logger)
    {
        _logger = logger;
        _serverContext = new(_testServer) { EndPoint = _endPoint };
        _clientContext = new(new ClientService<ITestService1>()) { EndPoint = _endPoint };
        logger.WriteLine($"{_endPoint}");
    }

    public interface ITestService1
    {
        void Invoke();
    }

    public interface ITestService2
    {
        void Invoke();
    }

    public interface ITestCallback2
    {
        void OnInvoked();
    }

    [Fact]
    public void Callback1_Test()
    {
        var manualResetEvent = new ManualResetEvent(false);
        _clientContext.Disconnected += ClientContext_Disconnected;
        _server!.Invoke();
        Assert.False(manualResetEvent.WaitOne(Timeout));

        void ClientContext_Disconnected(object? sender, EventArgs e)
        {
            manualResetEvent.Set();
        }
    }

    public async Task InitializeAsync()
    {
        _serverToken = await _serverContext.OpenAsync(CancellationToken.None);
        _logger.WriteLine($"Server is opened: {_serverToken}");
        _clientToken = await _clientContext.OpenAsync(CancellationToken.None);
        _logger.WriteLine($"Client is opened: {_clientToken}");
        _server = _testServer;
    }

    public async Task DisposeAsync()
    {
        await _serverContext.ReleaseAsync(_serverToken);
        _logger.WriteLine($"Server is released: {_serverToken}");
        await _clientContext.ReleaseAsync(_clientToken);
        _logger.WriteLine($"Client is released: {_clientToken}");
        _endPoint.Dispose();
    }

    private sealed class TestServer1 : ServerService<ITestService1, ITestCallback2>, ITestService1
    {
        public void Invoke() => Client.OnInvoked();
    }
}