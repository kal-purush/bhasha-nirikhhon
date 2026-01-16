using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Collections.Specialized;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Cida.Server.Models;
using Grpc.Core;
using Infrastructure;

namespace Cida.Server.Infrastructure
{
    public class InterNodeConnectionManager
    {
        private readonly Grpc.Core.Server server;
        private readonly IList<Grpc.Core.Channel> connections;
        private readonly IInfrastructureConfiguration configuration;
        private CidaInfrastructureService.CidaInfrastructureServiceClient client;

        public InterNodeConnectionManager(IInfrastructureConfiguration configuration)
        {
            this.configuration = configuration;
            this.server = new Grpc.Core.Server();
            this.server.Ports.Add(this.configuration.ServerEndpoint.Host, this.configuration.ServerEndpoint.Port,
                ServerCredentials.Insecure);
            var implementation = new CidaInfrastructureServiceImplementation();
            this.server.Services.Add(
                CidaInfrastructureService.BindService(implementation));

            implementation.OnSynchronize += ImplementationOnOnSynchronize;

            this.server.Start();

            this.connections = new List<Channel>();

            if (!string.IsNullOrEmpty(configuration.Node.Host) && configuration.Node.Port != default)
            {
                this.InitializeClient(configuration.Node);
            }
        }

        private void ImplementationOnOnSynchronize(Endpoint endpoint)
        {
            this.InitializeClient(endpoint);
        }

        private void InitializeClient(Endpoint endpoint)
        {
            // TODO: Add algorithm to use more than one other connection
            if (this.connections.Count == 0 && this.client == null)
            {
                this.connections.Add(new Channel(endpoint.Host, endpoint.Port, ChannelCredentials.Insecure));
                this.client = new CidaInfrastructureService.CidaInfrastructureServiceClient(this.connections[0]);
                this.client.Synchronize(new SynchronizeRequest()
                {
                    PublicEndpoint = new SynchronizeRequest.Types.Endpoint()
                    {
                        Host = this.configuration.ServerEndpoint.Host,
                        Port = this.configuration.ServerEndpoint.Port,
                    },
                });
            }
        }

        public class CidaInfrastructureServiceImplementation : CidaInfrastructureService.CidaInfrastructureServiceBase
        {
            public event Action<Endpoint> OnSynchronize;

            public override async Task<SynchronizeResponse> Synchronize(SynchronizeRequest request,
                ServerCallContext context)
            {
                this.OnSynchronize?.Invoke(request.PublicEndpoint);
                return await Task.FromResult(new SynchronizeResponse());
            }
        }
    }

    public interface IInfrastructureConfiguration
    {
        Endpoint ServerEndpoint { get; }
        Endpoint Node { get; }
    }
}