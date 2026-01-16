using api.Data;
using api.Interfaces;
using api.Shared;
using FluentValidation;
using MediatR;
using Microsoft.EntityFrameworkCore;

namespace api.Authentication.Commands;
public record AuthenticateCommand(string Email, string Password) : IRequest<Response>;

public class AuthenticateCommandValidator : AbstractValidator<AuthenticateCommand>
{
    public AuthenticateCommandValidator()
    {
        RuleFor(x => x.Email).NotEmpty().EmailAddress();
        RuleFor(x => x.Password).NotEmpty().MinimumLength(6);
    }
}
public class AuthenticateHandler : IRequestHandler<AuthenticateCommand, Response>
{
    private readonly DataContext _context;
    private readonly ITokenService _tokenService;
    private readonly IPasswordHelper _passwordHelper;

    public AuthenticateHandler(
        DataContext context,
        ITokenService tokenService,
        IPasswordHelper passwordHelper)
    {
        _context = context;
        _tokenService = tokenService;
        _passwordHelper = passwordHelper;
    }
    public async Task<Response> Handle(AuthenticateCommand request, CancellationToken cancellationToken)
    {
        var user = await _context.Users.Where(u => u.Email == request.Email).FirstOrDefaultAsync(cancellationToken);
        if (user is null)
        {
            return new Response("User not found", false);
        }

        if (!_passwordHelper.VerifyPassword(request.Password, user.Password ?? string.Empty))
        {
            return new Response("Password is incorrect", false);
        }

        var token = _tokenService.GenerateToken(user);
        return new Response("", true, new { token });
    }
}