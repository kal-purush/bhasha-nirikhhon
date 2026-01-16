using LiloDash.Domain.Core.Bus;
using Microsoft.Extensions.Configuration;
using System.Threading.Tasks;
using LiloDash.Domain.Core.Messages;
using System;
using EasyNetQ;
using RabbitMQ.Client.Exceptions;
using Polly;

namespace LiloDash.Infra.Bus
{
    public sealed class ServiceBrokerBus : IMessageHandler
    {
        #region :: Private Attributes

        private object _lock = new object();

        private readonly string _connectionString;

        private readonly IConfiguration _configuration;

        private IBus _bus;

        private IBus Bus
        {
            get 
            {
                TryConnect();
                return _bus;
            }
        }

        private IAdvancedBus _advancedBus
            => _bus.Advanced;

        #endregion

        #region :: Constructors

        public ServiceBrokerBus(IConfiguration configuration)
        {
            _configuration = configuration;
            _connectionString = _configuration.GetConnectionString("ServiceBroker");
        }

        #endregion

        #region :: Properties

        public bool IsConnected 
            => _advancedBus?.IsConnected ?? false;

        #endregion

        #region :: Publish 

        public Task Publish<TMessage>(TMessage message) where TMessage : BrokerMessage
            => Bus.PubSub.PublishAsync(message);

        public Task Publish<TMessage>(TMessage message, TimeSpan futurePublishTime) where TMessage : BrokerMessage
            => Bus.Scheduler.FuturePublishAsync(message, futurePublishTime);

        #endregion

        #region :: Request

        public Task<TResponse> Request<TRequest, TResponse>(TRequest request)
            where TRequest : BrokerMessage
            where TResponse : BrokerMessageResponse
            => Bus.Rpc.RequestAsync<TRequest, TResponse>(request); 

        #endregion

        #region :: Respond

        public Task<TResponse> Respond<TRequest, TResponse>(IBrokerMessageHandler<TRequest, TResponse> responder)
            where TRequest : BrokerMessage
            where TResponse : BrokerMessageResponse
            => throw new NotImplementedException();

        #endregion

        #region :: Subscribe

        public void Subscribe<TMessage>(IBrokerMessageHandler<TMessage> handler) 
            where TMessage : BrokerMessage
            => Bus
                .PubSub
                .SubscribeAsync(typeof(TMessage).Name, 
                    (TMessage message) => handler.Handle(message)); 

        #endregion

        #region :: Private Methods

        private void TryConnect()
        {
            lock (_lock)
            {
                if (IsConnected)
                    return;

                var policy = Policy.Handle<EasyNetQException>()
                .Or<BrokerUnreachableException>()
                .WaitAndRetry(4, retryAttempt =>
                    TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)));

                policy.Execute(() =>
                {
                    _bus = RabbitHutch.CreateBus(_connectionString);
                    _advancedBus.Disconnected += OnDisconnect;
                });
            }
        }

        private void OnDisconnect(object s, EventArgs e)
            => Policy.Handle<EasyNetQException>()
                .Or<BrokerUnreachableException>()
                .RetryForever()
                .Execute(TryConnect);

        #endregion

        #region :: Dispose

        public void Dispose()
            => _bus?.Dispose(); 

        #endregion
    }
}