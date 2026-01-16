using CSharpFunctionalExtensions;
using Microsoft.AspNetCore.Identity;
using Microsoft.Extensions.Logging;
using PetFamily.Accounts.Domain.DataModels;
using PetFamily.Core.Abstractions;
using PetFamily.SharedKernel.CustomErrors;

namespace PetFamily.Accounts.Application.Commands.Login;

public class LoginUserHandler : ICommandHandler<string, LoginUserCommand>
{
    private readonly UserManager<User> _userManager;
    private readonly ILogger<LoginUserHandler> _logger;
    private readonly ITokenProvider _tokenProvider;

    public LoginUserHandler(
        UserManager<User> userManager,
        ILogger<LoginUserHandler> logger,
        ITokenProvider tokenProvider)
    {
        _userManager = userManager;
        _logger = logger;
        _tokenProvider = tokenProvider;
    }

    public async Task<Result<string, ErrorList>> HandleAsync(
        LoginUserCommand command,
        CancellationToken cancellationToken = default)
    {
        var user = await _userManager.FindByEmailAsync(command.Email);
        if (user is null)
            return Errors.General.ValueNotFound("No user with such login data", true).ToErrorList();
        
        var passwordConfirmed = await _userManager.CheckPasswordAsync(user, command.Password);
        if (!passwordConfirmed)
            return Errors.General.ValueNotFound("No user with such login data", true).ToErrorList();

        var token =  _tokenProvider.GenerateAccessToken(user);
        
        _logger.LogInformation("User with email = {0} successfully logged in", user.Email);

        return token;


    }
}