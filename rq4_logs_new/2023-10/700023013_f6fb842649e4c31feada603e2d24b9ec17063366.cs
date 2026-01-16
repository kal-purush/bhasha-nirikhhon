using AutoMapper;
using MediatR;

namespace Application.Commons.Mediator;

public abstract class BaseHandler<TRequest, TResponse> : IRequestHandler<TRequest, TResponse> 
    where TRequest : BaseRequest<TResponse>, IRequest<TResponse>
    where TResponse : BaseResponse
{
    protected readonly IMapper _mapper;
    protected readonly AppDbContext _ctx;

    public BaseHandler(IMapper mapper, AppDbContext ctx)
    {
        _mapper = mapper;
        _ctx = ctx;
    }

    public async Task<TResponse> Handle(TRequest request, CancellationToken cancellationToken)
    {
        Console.WriteLine($"HostUrl = {request.HostUrl}");
        Console.WriteLine($"UserId = {request.UserId}");
        Console.WriteLine($"LoggedUserId = {request.LoggedUserId}");
        Console.WriteLine($"IsUserIdProvided = {request.IsUserIdProvided}");
        Console.WriteLine("\n\n");

        if (request.IsUserIdProvided && request.UserId != 0 && request.UserId != request.LoggedUserId) 
            throw new UnauthorizedAccessException("Given user id is not valid");

        return await Execute(request, cancellationToken);
    }

    public abstract Task<TResponse> Execute(TRequest request, CancellationToken cancellationToken);
}