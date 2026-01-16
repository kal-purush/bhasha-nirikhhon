using CSharpFunctionalExtensions;
using DeliveryApp.Core.Domain.SharedKernel.Exceptions;
using DeliveryApp.Core.Ports;
using GeoApp.Api;
using Grpc.Core;
using Grpc.Net.Client;
using Grpc.Net.Client.Configuration;
using Location = DeliveryApp.Core.Domain.SharedKernel.Location;

namespace DeliveryApp.Infrastructure.Adapters.Grpc.GeoService;

public class Client : IGeoClient
{
    private readonly string _url;
    private readonly SocketsHttpHandler _socketsHttpHandler;
    private readonly MethodConfig _methodConfig;

    public Client(string url)
    {
        _url = url;
        
        _socketsHttpHandler = new SocketsHttpHandler
        {
            PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
            KeepAlivePingDelay = TimeSpan.FromSeconds(60),
            KeepAlivePingTimeout = TimeSpan.FromSeconds(30),
            EnableMultipleHttp2Connections = true
        };

        _methodConfig = new MethodConfig
        {
            Names = { MethodName.Default },
            RetryPolicy = new RetryPolicy
            {
                MaxAttempts = 5,
                InitialBackoff = TimeSpan.FromSeconds(1),
                MaxBackoff = TimeSpan.FromSeconds(5),
                BackoffMultiplier = 1.5,
                RetryableStatusCodes = { StatusCode.Unavailable }
            }
        };
    }
    
    public async Task<Result<Location, DomainException>> Location(string address, CancellationToken cancellationToken)
    {
        using var channel = GrpcChannel.ForAddress(_url, new GrpcChannelOptions
        {
            HttpHandler = _socketsHttpHandler,
            ServiceConfig = new ServiceConfig { MethodConfigs = { _methodConfig } }
        });

        var client = new Geo.GeoClient(channel);

        try
        {
            var result = await client.GetGeolocationAsync(new GetGeolocationRequest
            {
                Address = address
            }, null, DateTime.UtcNow.AddSeconds(5), cancellationToken: cancellationToken);

            return Core.Domain.SharedKernel.Location.Of(result.Location.X, result.Location.Y)
                       .MapError(x => (DomainException) x);
        }
        catch (RpcException)
        {
            return new DomainException("Couldn't get location");
        }
    }
}