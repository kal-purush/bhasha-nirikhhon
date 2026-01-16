using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;

namespace Mediator.Switch;

#pragma warning disable CS1998

public interface ISwitchMediatorServiceProvider
{
    T Get<T>() where T : notnull;
}

public class MicrosoftDependencyInjectionServiceProvider : ISwitchMediatorServiceProvider
{
    private readonly IServiceProvider _serviceProvider;

    public MicrosoftDependencyInjectionServiceProvider(IServiceProvider serviceProvider) => 
        _serviceProvider = serviceProvider;

    public T Get<T>() where T : notnull => 
        _serviceProvider.GetRequiredService<T>();
}

public class SwitchMediator2 : IMediator
{
    #region Fields
    
    private Sample.ConsoleApp.GetUserRequestHandler? _sample_ConsoleApp_GetUserRequestHandler;
    private Sample.ConsoleApp.CreateOrderRequestHandler? _sample_ConsoleApp_CreateOrderRequestHandler;
    private Sample.ConsoleApp.AnimalRequestHandler? _sample_ConsoleApp_AnimalRequestHandler;
    private Sample.ConsoleApp.VersionIncrementingBehavior<Sample.ConsoleApp.GetUserRequest, Sample.ConsoleApp.User>? _sample_ConsoleApp_VersionIncrementingBehavior__sample_ConsoleApp_GetUserRequest;
    private Sample.ConsoleApp.AuditBehavior<Sample.ConsoleApp.GetUserRequest, FluentResults.Result<Sample.ConsoleApp.User>>? _sample_ConsoleApp_AuditBehavior__sample_ConsoleApp_GetUserRequest;
    private Sample.ConsoleApp.ValidationBehavior<Sample.ConsoleApp.GetUserRequest, FluentResults.Result<Sample.ConsoleApp.User>>? _sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_GetUserRequest;
    private Sample.ConsoleApp.LoggingBehavior<Sample.ConsoleApp.GetUserRequest, FluentResults.Result<Sample.ConsoleApp.User>>? _sample_ConsoleApp_LoggingBehavior__sample_ConsoleApp_GetUserRequest;
    private Sample.ConsoleApp.TransactionBehavior<Sample.ConsoleApp.CreateOrderRequest, int>? _sample_ConsoleApp_TransactionBehavior__sample_ConsoleApp_CreateOrderRequest;
    private Sample.ConsoleApp.ValidationBehavior<Sample.ConsoleApp.CreateOrderRequest, int>? _sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_CreateOrderRequest;
    private Sample.ConsoleApp.ValidationBehavior<Sample.ConsoleApp.Animal, Mediator.Switch.Unit>? _sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_Animal;
    private Sample.ConsoleApp.ValidationBehavior<Sample.ConsoleApp.Dog, Mediator.Switch.Unit>? _sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_Dog;
    private Sample.ConsoleApp.ValidationBehavior<Sample.ConsoleApp.Cat, Mediator.Switch.Unit>? _sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_Cat;
    private IEnumerable<INotificationHandler<Sample.ConsoleApp.UserLoggedInEvent>>? _sample_ConsoleApp_UserLoggedInEvent__Handlers;
    private IEnumerable<INotificationHandler<Sample.ConsoleApp.DerivedUserLoggedInEvent>>? _sample_ConsoleApp_DerivedUserLoggedInEvent__Handlers;

    #endregion
    
    private readonly ISwitchMediatorServiceProvider _svc;

    public SwitchMediator2(ISwitchMediatorServiceProvider serviceProvider)
    {
        _svc = serviceProvider;
    }

    public static (IReadOnlyList<Type> RequestHandlerTypes, IReadOnlyList<Type> NotificationHandlerTypes, IReadOnlyList<Type> PipelineBehaviorTypes) KnownTypes
    {
        get { return (SwitchMediator2KnownTypes.RequestHandlerTypes, SwitchMediator2KnownTypes.NotificationHandlerTypes, SwitchMediator2KnownTypes.PipelineBehaviorTypes); }
    }

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
            { // case Sample.ConsoleApp.Cat:
                typeof(Sample.ConsoleApp.Cat), (instance, request, cancellationToken) =>
                    instance.Handle_Sample_ConsoleApp_Animal(
                        (Sample.ConsoleApp.Cat) request, cancellationToken)
            },
            { // case Sample.ConsoleApp.Dog:
                typeof(Sample.ConsoleApp.Dog), (instance, request, cancellationToken) =>
                    instance.Handle_Sample_ConsoleApp_Animal(
                        (Sample.ConsoleApp.Dog) request, cancellationToken)
            },
            { // case Sample.ConsoleApp.Animal:
                typeof(Sample.ConsoleApp.Animal), (instance, request, cancellationToken) =>
                    instance.Handle_Sample_ConsoleApp_Animal(
                        (Sample.ConsoleApp.Animal) request, cancellationToken)
            },
            { // case Sample.ConsoleApp.CreateOrderRequest:
                typeof(Sample.ConsoleApp.CreateOrderRequest), (instance, request, cancellationToken) =>
                    instance.Handle_Sample_ConsoleApp_CreateOrderRequest(
                        (Sample.ConsoleApp.CreateOrderRequest) request, cancellationToken)
            },
            { // case Sample.ConsoleApp.GetUserRequest:
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
            { // case Sample.ConsoleApp.DerivedUserLoggedInEvent:
                typeof(Sample.ConsoleApp.DerivedUserLoggedInEvent), async (instance, notification, cancellationToken) =>
                {
                    foreach (var handler in instance.Get(ref instance._sample_ConsoleApp_UserLoggedInEvent__Handlers))
                    {
                        await handler.Handle((Sample.ConsoleApp.DerivedUserLoggedInEvent)notification, cancellationToken);
                    }
                }
            },
            { // case Sample.ConsoleApp.UserLoggedInEvent:
                typeof(Sample.ConsoleApp.UserLoggedInEvent), async (instance, notification, cancellationToken) =>
                {
                    foreach (var handler in instance.Get(ref instance._sample_ConsoleApp_UserLoggedInEvent__Handlers))
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
            Get(ref _sample_ConsoleApp_LoggingBehavior__sample_ConsoleApp_GetUserRequest).Handle(request, () =>
            Get(ref _sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_GetUserRequest).Handle(request, () =>
            Get(ref _sample_ConsoleApp_AuditBehavior__sample_ConsoleApp_GetUserRequest).Handle(request, () =>
            Get(ref _sample_ConsoleApp_VersionIncrementingBehavior__sample_ConsoleApp_GetUserRequest).Handle(request, () =>
            /* Request Handler */ Get(ref _sample_ConsoleApp_GetUserRequestHandler).Handle(request, cancellationToken),
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
            Get(ref _sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_CreateOrderRequest).Handle(request, () =>
            Get(ref _sample_ConsoleApp_TransactionBehavior__sample_ConsoleApp_CreateOrderRequest).Handle(request, () =>
            /* Request Handler */ Get(ref _sample_ConsoleApp_CreateOrderRequestHandler).Handle(request, cancellationToken),
            cancellationToken),
            cancellationToken);
    }

    private Task<Mediator.Switch.Unit> Handle_Sample_ConsoleApp_Animal(
        Sample.ConsoleApp.Animal request,
        CancellationToken cancellationToken)
    {
        return
            Get(ref _sample_ConsoleApp_ValidationBehavior__sample_ConsoleApp_Animal).Handle(request, () =>
            /* Request Handler */ Get(ref _sample_ConsoleApp_AnimalRequestHandler).Handle(request, cancellationToken),
            cancellationToken);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [DebuggerStepThrough]
    private T Get<T>(ref T? field) where T : notnull
    {
        return field ?? (field = _svc.Get<T>());
    }

    /// <summary>
    /// Provides lists of SwitchMediator2 component implementation types.
    /// </summary>
    public static class SwitchMediator2KnownTypes
    {
        public static readonly IReadOnlyList<Type> RequestHandlerTypes =
            new Type[] {
                typeof(Sample.ConsoleApp.GetUserRequestHandler),
                typeof(Sample.ConsoleApp.CreateOrderRequestHandler),
                typeof(Sample.ConsoleApp.AnimalRequestHandler)
            }.AsReadOnly();

        public static readonly IReadOnlyList<Type> NotificationHandlerTypes =
           new Type[] {
                typeof(Sample.ConsoleApp.UserLoggedInLogger),
                typeof(Sample.ConsoleApp.UserLoggedInAnalytics)
           }.AsReadOnly();

        public static readonly IReadOnlyList<Type> PipelineBehaviorTypes =
           new Type[] {
                typeof(Sample.ConsoleApp.LoggingBehavior<,>),
                typeof(Sample.ConsoleApp.ValidationBehavior<,>),
                typeof(Sample.ConsoleApp.AuditBehavior<,>),
                typeof(Sample.ConsoleApp.VersionIncrementingBehavior<,>),
                typeof(Sample.ConsoleApp.TransactionBehavior<,>)
           }.AsReadOnly();
    }
}