using api.Data;
using api.Shared;
using MediatR;
using Microsoft.EntityFrameworkCore;

namespace api.Cryptos.Queries;
public record GetCryptosCommand() : IRequest<Response>;
public class GetCryptosCommandHandler : IRequestHandler<GetCryptosCommand, Response>
{
    private readonly DataContext _context;

    public GetCryptosCommandHandler(
        DataContext context)
    {
        _context = context;
    }

    public async Task<Response> Handle(GetCryptosCommand request, CancellationToken cancellationToken)
    {
        var cryptos = await _context.Cryptos
                              .OrderByDescending(c => c.Name)
                              .ToListAsync(cancellationToken); ;
        return new Response("", true, cryptos);
    }
}