using System;
using System.Threading.Tasks;
using FluentValidation.Results;
using LiloDash.Domain.Core.Commands;
using LiloDash.Domain.Core.Messages;

namespace LiloDash.Domain.Core.Bus
{
    ///<summary>
    /// Blackboard pattern Handler
    ///</summary>
    public interface IMessageHandler: IDisposable
    {
        ///<summary>
        /// Returns true if service broker server is connected
        ///</summary>
        bool IsConnected { get; }

        #region :: Publish Events

        Task Publish<TMessage>(TMessage message)
            where TMessage : BrokerMessage;

        Task Publish<TMessage>(TMessage message, TimeSpan futurePublishTime)
            where TMessage : BrokerMessage;

        Task<TResponse> Request<TRequest, TResponse>(TRequest request)
            where TRequest : BrokerMessage
            where TResponse : BrokerMessageResponse;

        #endregion

        #region :: Catch Events

        void Subscribe<TMessage>(IBrokerMessageHandler<TMessage> handler)
            where TMessage : BrokerMessage;

        Task<TResponse> Respond<TRequest, TResponse>(IBrokerMessageHandler<TRequest, TResponse> responder)
            where TRequest : BrokerMessage
            where TResponse : BrokerMessageResponse;
        #endregion
    }
}