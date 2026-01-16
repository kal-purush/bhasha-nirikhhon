using AutoMapper;
using MediatR;

namespace Application.Commons;

public abstract class BaseRequestHandler<TRequest, TResponse> : IRequestHandler<TRequest, TResponse> where TRequest : IRequest<TResponse>
{
    protected readonly IMapper _mapper;
    protected readonly AppDbContext _ctx;

    public BaseRequestHandler(IMapper mapper, AppDbContext ctx)
    {
        _mapper = mapper;
        _ctx = ctx;
    }

    public abstract Task<TResponse> Handle(TRequest request, CancellationToken cancellationToken);
}