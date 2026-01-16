using FluentResults;
using MediatR;

namespace Codend.Application.Abstractions.Messaging.Commands;

/// <summary>
/// Interface of command handler without response type
/// </summary>
/// <typeparam name="TCommand">Type of handled command</typeparam>
public interface ICommandHandler<TCommand> : IRequestHandler<TCommand, Result> where TCommand : ICommand
{
}

/// <summary>
/// Interface of command handler
/// </summary>
/// <typeparam name="TCommand">Type of handled command</typeparam>
/// <typeparam name="TResposne">Result response type</typeparam>
public interface ICommandHandler<TCommand, TResposne> : IRequestHandler<TCommand, Result<TResposne>> where TCommand : ICommand<TResposne>
{
}