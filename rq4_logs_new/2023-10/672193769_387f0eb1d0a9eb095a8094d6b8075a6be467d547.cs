using api.Data;
using api.Shared;
using api.Users.Dtos;
using MediatR;
using Microsoft.EntityFrameworkCore;

namespace api.Cryptos.Queries;
public record GetUserByIdCommand(int UserId) : IRequest<Response>;
public class GetUserByIdCommandHandler : IRequestHandler<GetUserByIdCommand, Response>
{
    private readonly DataContext _context;

    public GetUserByIdCommandHandler(DataContext context)
    {
        _context = context;
    }

    public async Task<Response> Handle(GetUserByIdCommand request, CancellationToken cancellationToken)
    {
        var user = await _context.Users
                                        .Where(x => x.Id == request.UserId)
                                        .Select(x => new UserDto(x.Id,
                                                                 x.FirstName,
                                                                 x.LastName,
                                                                 x.Email,
                                                                 x.Role))
                                        .FirstOrDefaultAsync(cancellationToken);
        if (user is null)
            return new Response("User not found", false);

        return new Response("", true, user);
    }
}