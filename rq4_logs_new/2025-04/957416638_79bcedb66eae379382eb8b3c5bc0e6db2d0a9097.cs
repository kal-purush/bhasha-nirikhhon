using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Mediator.Switch;

#pragma warning disable CS1998

public class SwitchMediator2 : IMediator
{
    #region Fields

    private readonly Lazy<Sample.ConsoleApp.GetUserRequestHandler> _sample_ConsoleApp_GetUserRequestHandler;
    private readonly Sample.ConsoleApp.CreateOrderRequestHandler _sample_ConsoleApp_CreateOrderRequestHandler;
    private readonly Sample.ConsoleApp.AnimalRequestHandler _sample_ConsoleApp_AnimalRequestHandler;
    private readonly Sample.ConsoleApp.VersionIncrementingBehavior<Sample.ConsoleApp.GetUserRequest, Sample.ConsoleApp.User> _sample_ConsoleApp_VersionIncrementingBehavior__sample_ConsoleApp_GetUserRequest;
    private readonly Sample.ConsoleApp.AuditBehavior<Sample.ConsoleApp.GetUserRequest, FluentResults.Result<Sample.ConsoleApp.User>> _sample_ConsoleApp_AuditBehavior__sample_ConsoleApp_GetUserRequest;
    private readonly Sample.ConsoleApp.ValidationBehavior<Sample.ConsoleApp.GetUserRequest, FluentResults.Result<Sample.ConsoleApp.User>> _sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_GetUserRequest;
    private readonly Sample.ConsoleApp.LoggingBehavior<Sample.ConsoleApp.GetUserRequest, FluentResults.Result<Sample.ConsoleApp.User>> _sample_ConsoleApp_LoggingBehavior__sample_ConsoleApp_GetUserRequest;
    private readonly Sample.ConsoleApp.TransactionBehavior<Sample.ConsoleApp.CreateOrderRequest, int> _sample_ConsoleApp_TransactionBehavior__sample_ConsoleApp_CreateOrderRequest;
    private readonly Sample.ConsoleApp.ValidationBehavior<Sample.ConsoleApp.CreateOrderRequest, int> _sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_CreateOrderRequest;
    private readonly Sample.ConsoleApp.ValidationBehavior<Sample.ConsoleApp.Animal, Mediator.Switch.Unit> _sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_Animal;
    private readonly Sample.ConsoleApp.ValidationBehavior<Sample.ConsoleApp.Dog, Mediator.Switch.Unit> _sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_Dog;
    private readonly Sample.ConsoleApp.ValidationBehavior<Sample.ConsoleApp.Cat, Mediator.Switch.Unit> _sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_Cat;
    private readonly IEnumerable<INotificationHandler<Sample.ConsoleApp.UserLoggedInEvent>> _sample_ConsoleApp_UserLoggedInEvent__Handlers;

    #endregion

    #region Constructor

    public SwitchMediator2(
        Lazy<Sample.ConsoleApp.GetUserRequestHandler> sample_ConsoleApp_GetUserRequestHandler,
        Sample.ConsoleApp.CreateOrderRequestHandler sample_ConsoleApp_CreateOrderRequestHandler,
        Sample.ConsoleApp.AnimalRequestHandler sample_ConsoleApp_AnimalRequestHandler,
        Sample.ConsoleApp.VersionIncrementingBehavior<Sample.ConsoleApp.GetUserRequest, Sample.ConsoleApp.User> sample_ConsoleApp_VersionIncrementingBehavior__sample_ConsoleApp_GetUserRequest,
        Sample.ConsoleApp.AuditBehavior<Sample.ConsoleApp.GetUserRequest, FluentResults.Result<Sample.ConsoleApp.User>> sample_ConsoleApp_AuditBehavior__sample_ConsoleApp_GetUserRequest,
        Sample.ConsoleApp.ValidationBehavior<Sample.ConsoleApp.GetUserRequest, FluentResults.Result<Sample.ConsoleApp.User>> sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_GetUserRequest,
        Sample.ConsoleApp.LoggingBehavior<Sample.ConsoleApp.GetUserRequest, FluentResults.Result<Sample.ConsoleApp.User>> sample_ConsoleApp_LoggingBehavior__sample_ConsoleApp_GetUserRequest,
        Sample.ConsoleApp.TransactionBehavior<Sample.ConsoleApp.CreateOrderRequest, int> sample_ConsoleApp_TransactionBehavior__sample_ConsoleApp_CreateOrderRequest,
        Sample.ConsoleApp.ValidationBehavior<Sample.ConsoleApp.CreateOrderRequest, int> sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_CreateOrderRequest,
        Sample.ConsoleApp.ValidationBehavior<Sample.ConsoleApp.Animal, Mediator.Switch.Unit> sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_Animal,
        Sample.ConsoleApp.ValidationBehavior<Sample.ConsoleApp.Dog, Mediator.Switch.Unit> sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_Dog,
        Sample.ConsoleApp.ValidationBehavior<Sample.ConsoleApp.Cat, Mediator.Switch.Unit> sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_Cat,
        IEnumerable<INotificationHandler<Sample.ConsoleApp.UserLoggedInEvent>> sample_ConsoleApp_UserLoggedInEvent__Handlers)
    {
        _sample_ConsoleApp_GetUserRequestHandler = sample_ConsoleApp_GetUserRequestHandler;
        _sample_ConsoleApp_CreateOrderRequestHandler = sample_ConsoleApp_CreateOrderRequestHandler;
        _sample_ConsoleApp_AnimalRequestHandler = sample_ConsoleApp_AnimalRequestHandler;
        _sample_ConsoleApp_VersionIncrementingBehavior__sample_ConsoleApp_GetUserRequest = sample_ConsoleApp_VersionIncrementingBehavior__sample_ConsoleApp_GetUserRequest;
        _sample_ConsoleApp_AuditBehavior__sample_ConsoleApp_GetUserRequest = sample_ConsoleApp_AuditBehavior__sample_ConsoleApp_GetUserRequest;
        _sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_GetUserRequest = sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_GetUserRequest;
        _sample_ConsoleApp_LoggingBehavior__sample_ConsoleApp_GetUserRequest = sample_ConsoleApp_LoggingBehavior__sample_ConsoleApp_GetUserRequest;
        _sample_ConsoleApp_TransactionBehavior__sample_ConsoleApp_CreateOrderRequest = sample_ConsoleApp_TransactionBehavior__sample_ConsoleApp_CreateOrderRequest;
        _sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_CreateOrderRequest = sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_CreateOrderRequest;
        _sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_Animal = sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_Animal;
        _sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_Dog = sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_Dog;
        _sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_Cat = sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_Cat;
        _sample_ConsoleApp_UserLoggedInEvent__Handlers = sample_ConsoleApp_UserLoggedInEvent__Handlers;
    }

    #endregion

    public Task<TResponse> Send<TResponse>(IRequest<TResponse> request, CancellationToken cancellationToken = default)
    {
        if (SendSwitchCase.Cases.TryGetValue(request.GetType(), out var handle))
        {
            return (Task<TResponse>)handle(this, request, cancellationToken);
        }
        
        throw new ArgumentException($"No handler for {request.GetType().Name}");
    }

    private static class SendSwitchCase
    {
        public static readonly Dictionary<Type, Func<SwitchMediator2, object, CancellationToken, object>> Cases = new Dictionary<Type, Func<SwitchMediator2, object, CancellationToken, object>>
        {
            { // case
                typeof(Sample.ConsoleApp.Cat), (instance, request, cancellationToken) =>
                    instance.Handle_Sample_ConsoleApp_Animal(
                        (Sample.ConsoleApp.Animal) request, cancellationToken)
            },
            { // case
                typeof(Sample.ConsoleApp.Dog), (instance, request, cancellationToken) =>
                    instance.Handle_Sample_ConsoleApp_Animal(
                        (Sample.ConsoleApp.Animal) request, cancellationToken)
            },
            { // case
                typeof(Sample.ConsoleApp.CreateOrderRequest), (instance, request, cancellationToken) =>
                    instance.Handle_Sample_ConsoleApp_CreateOrderRequest(
                        (Sample.ConsoleApp.CreateOrderRequest) request, cancellationToken)
            },
            { // case
                typeof(Sample.ConsoleApp.GetUserRequest), (instance, request, cancellationToken) =>
                    instance.Handle_Sample_ConsoleApp_GetUserRequest(
                        (Sample.ConsoleApp.GetUserRequest) request, cancellationToken)
            }
        };
    }

    public Task Publish(INotification notification, CancellationToken cancellationToken = default)
    {
        if (PublishSwitchCase.Cases.TryGetValue(notification.GetType(), out var handle))
        {
            return handle(this, notification, cancellationToken);
        }
        
        throw new ArgumentException($"No handler for {notification.GetType().Name}");
    }

    private static class PublishSwitchCase
    {
        public static readonly Dictionary<Type, Func<SwitchMediator2, INotification, CancellationToken, Task>> Cases = new Dictionary<Type, Func<SwitchMediator2, INotification, CancellationToken, Task>>
        {
            { // case
                typeof(Sample.ConsoleApp.UserLoggedInEvent), async (instance, notification, cancellationToken) =>
                {
                    foreach (var handler in instance._sample_ConsoleApp_UserLoggedInEvent__Handlers)
                    {
                        await handler.Handle((Sample.ConsoleApp.UserLoggedInEvent)notification, cancellationToken);
                    }
                }
            }
        };
    }

    private Task<FluentResults.Result<Sample.ConsoleApp.User>> Handle_Sample_ConsoleApp_GetUserRequest(
        Sample.ConsoleApp.GetUserRequest request,
        CancellationToken cancellationToken)
    {
        return
            _sample_ConsoleApp_LoggingBehavior__sample_ConsoleApp_GetUserRequest.Handle(request, () =>
            _sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_GetUserRequest.Handle(request, () =>
            _sample_ConsoleApp_AuditBehavior__sample_ConsoleApp_GetUserRequest.Handle(request, () =>
            _sample_ConsoleApp_VersionIncrementingBehavior__sample_ConsoleApp_GetUserRequest.Handle(request, () =>
            /* Request Handler */ _sample_ConsoleApp_GetUserRequestHandler.Value.Handle(request, cancellationToken),
            cancellationToken),
            cancellationToken),
            cancellationToken),
            cancellationToken);
    }

    private Task<int> Handle_Sample_ConsoleApp_CreateOrderRequest(
        Sample.ConsoleApp.CreateOrderRequest request,
        CancellationToken cancellationToken)
    {
        return
            _sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_CreateOrderRequest.Handle(request, () =>
            _sample_ConsoleApp_TransactionBehavior__sample_ConsoleApp_CreateOrderRequest.Handle(request, () =>
            /* Request Handler */ _sample_ConsoleApp_CreateOrderRequestHandler.Handle(request, cancellationToken),
            cancellationToken),
            cancellationToken);
    }
    
    private Task<Mediator.Switch.Unit> Handle_Sample_ConsoleApp_Animal(
        Sample.ConsoleApp.Animal request,
        CancellationToken cancellationToken)
    {
        return
            _sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_Animal.Handle(request, () =>
                    /* Request Handler */ _sample_ConsoleApp_AnimalRequestHandler.Handle(request, cancellationToken),
                cancellationToken);
    }
}