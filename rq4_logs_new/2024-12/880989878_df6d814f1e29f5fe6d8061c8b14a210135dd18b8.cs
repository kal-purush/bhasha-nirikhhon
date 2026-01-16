using MarGate.Core.CQRS.Command;
using MarGate.Core.UnitOfWork.UnitOfWork;
using MediatR;
using Microsoft.Extensions.DependencyInjection;

namespace MarGate.Core.CQRS.Behavior;

public class CommitTransactionPipelineBehavior<TRequest, TResponse>(IServiceProvider serviceProvider) : IPipelineBehavior<TRequest, TResponse> where TRequest : IRequest<TResponse>
{
    public async Task<TResponse> Handle(TRequest request, RequestHandlerDelegate<TResponse> next, CancellationToken cancellationToken)
    {
        var response = await next();

        if (typeof(ICommand).IsAssignableFrom(request.GetType()))
        {
            var unitOfWork = serviceProvider.GetService<IUnitOfWork>();
            if (unitOfWork is null)
            {
                return response;
            }

            await unitOfWork.SaveChangesAsync(cancellationToken);
        }

        return response;
    }
}